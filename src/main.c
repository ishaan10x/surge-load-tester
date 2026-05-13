#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <getopt.h>
#include <time.h>
#include <signal.h>

#define MAX_EVENTS 1024
#define BUFFER_SIZE 4096
#define CACHE_LINE 64
#define HISTOGRAM_BUCKETS 100000
#define BUCKET_RESOLUTION_US 100

typedef enum {
    STATE_CONNECTING,
    STATE_WRITING,
    STATE_READING,
    STATE_DONE
} ConnectionState;

typedef struct {
    int fd;
    ConnectionState state;
    size_t bytes_written;
    uint64_t start_time_us;
} Connection;

typedef struct __attribute__((aligned(CACHE_LINE))) {
    int thread_id;
    int epoll_fd;
    int concurrency;
    Connection *connections;
    struct addrinfo *res;
    char *request;
    int req_len;
    int active_connections;
    unsigned int histogram[HISTOGRAM_BUCKETS];
    bool chaos_mode;
    unsigned int prng_seed;
} ThreadContext;

atomic_int total_completed = 0;
atomic_int total_failed = 0;
atomic_bool keep_running = true;
unsigned int global_histogram[HISTOGRAM_BUCKETS] = {0};

uint64_t get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000ULL);
}

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) exit(EXIT_FAILURE);
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) exit(EXIT_FAILURE);
}

void reset_connection(ThreadContext *ctx, Connection *conn) {
    if (conn->fd > 0) close(conn->fd);
    
    conn->fd = socket(ctx->res->ai_family, ctx->res->ai_socktype, ctx->res->ai_protocol);
    if (conn->fd == -1) {
        conn->state = STATE_DONE;
        ctx->active_connections--;
        return;
    }
    
    set_nonblocking(conn->fd);
    conn->state = STATE_CONNECTING;
    conn->bytes_written = 0;
    conn->start_time_us = 0;

    if (connect(conn->fd, ctx->res->ai_addr, ctx->res->ai_addrlen) == -1 && errno != EINPROGRESS) {
        close(conn->fd);
        conn->state = STATE_DONE;
        ctx->active_connections--;
        return;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.ptr = conn;
    epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, conn->fd, &ev);
}

void maximize_fd_limit() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

void handle_sigint(int sig) {
    (void)sig;
    atomic_store(&keep_running, false);
}

void *worker_thread(void *arg) {
    ThreadContext *ctx = (ThreadContext *)arg;
    struct epoll_event events[MAX_EVENTS];

    for (int j = 0; j < ctx->concurrency; j++) {
        ctx->connections[j].fd = -1;
        reset_connection(ctx, &ctx->connections[j]);
    }

    while (ctx->active_connections > 0) {
        if (!atomic_load_explicit(&keep_running, memory_order_relaxed)) {
            break;
        }

        int nfds = epoll_wait(ctx->epoll_fd, events, MAX_EVENTS, 100);
        if (nfds == -1) break;

        for (int n = 0; n < nfds; n++) {
            Connection *conn = (Connection *)events[n].data.ptr;

            if (events[n].events & (EPOLLERR | EPOLLHUP)) {
                atomic_fetch_add_explicit(&total_failed, 1, memory_order_relaxed);
                if (atomic_load_explicit(&keep_running, memory_order_relaxed)) {
                    reset_connection(ctx, conn);
                } else {
                    close(conn->fd);
                    conn->state = STATE_DONE;
                    ctx->active_connections--;
                }
                continue;
            }

            if (conn->state == STATE_CONNECTING && (events[n].events & EPOLLOUT)) {
                int err = 0;
                socklen_t len = sizeof(err);
                if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &err, &len) == -1 || err != 0) {
                    atomic_fetch_add_explicit(&total_failed, 1, memory_order_relaxed);
                    if (atomic_load_explicit(&keep_running, memory_order_relaxed)) {
                        reset_connection(ctx, conn);
                    } else {
                        close(conn->fd);
                        conn->state = STATE_DONE;
                        ctx->active_connections--;
                    }
                    continue;
                }
                conn->state = STATE_WRITING;
                conn->start_time_us = get_time_us();
            }

            if (conn->state == STATE_WRITING && (events[n].events & EPOLLOUT)) {
                int write_error = 0;
                while (conn->bytes_written < (size_t)ctx->req_len) {
                    size_t to_send = ctx->req_len - conn->bytes_written;
                    
                    if (ctx->chaos_mode && to_send > 1) {
                        to_send = 1 + (rand_r(&ctx->prng_seed) % (to_send / 2 + 1));
                    }

                    ssize_t sent = send(conn->fd, ctx->request + conn->bytes_written, to_send, 0);
                    if (sent == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        write_error = 1;
                        break;
                    }
                    conn->bytes_written += sent;
                }

                if (write_error) {
                    atomic_fetch_add_explicit(&total_failed, 1, memory_order_relaxed);
                    if (atomic_load_explicit(&keep_running, memory_order_relaxed)) {
                        reset_connection(ctx, conn);
                    } else {
                        close(conn->fd);
                        conn->state = STATE_DONE;
                        ctx->active_connections--;
                    }
                    continue;
                }

                if (conn->bytes_written == (size_t)ctx->req_len) {
                    conn->state = STATE_READING;
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.ptr = conn;
                    epoll_ctl(ctx->epoll_fd, EPOLL_CTL_MOD, conn->fd, &ev);
                }
            }

            if (conn->state == STATE_READING && (events[n].events & EPOLLIN)) {
                char buffer[BUFFER_SIZE];
                while (1) {
                    ssize_t bytes_read = recv(conn->fd, buffer, sizeof(buffer), 0);
                    if (bytes_read == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        atomic_fetch_add_explicit(&total_failed, 1, memory_order_relaxed);
                        if (atomic_load_explicit(&keep_running, memory_order_relaxed)) {
                            reset_connection(ctx, conn);
                        } else {
                            close(conn->fd);
                            conn->state = STATE_DONE;
                            ctx->active_connections--;
                        }
                        break;
                    } else if (bytes_read == 0) {
                        uint64_t end_time_us = get_time_us();
                        if (conn->start_time_us > 0) {
                            uint64_t latency_us = end_time_us - conn->start_time_us;
                            uint64_t bucket = latency_us / BUCKET_RESOLUTION_US;
                            if (bucket >= HISTOGRAM_BUCKETS) bucket = HISTOGRAM_BUCKETS - 1;
                            ctx->histogram[bucket]++;
                        }
                        
                        atomic_fetch_add_explicit(&total_completed, 1, memory_order_relaxed);
                        if (atomic_load_explicit(&keep_running, memory_order_relaxed)) {
                            reset_connection(ctx, conn);
                        } else {
                            close(conn->fd);
                            conn->state = STATE_DONE;
                            ctx->active_connections--;
                        }
                        break;
                    }
                }
            }
        }
    }

    for (int j = 0; j < ctx->concurrency; j++) {
        if (ctx->connections[j].state != STATE_DONE && ctx->connections[j].fd != -1) {
            close(ctx->connections[j].fd);
            ctx->connections[j].state = STATE_DONE;
        }
    }

    return NULL;
}

void print_report(int total_reqs, double total_duration) {
    printf("\n\n--- Test Summary ---\n");
    printf("Total Requests: %d\n", total_reqs);
    printf("Total Failed:   %d\n", atomic_load(&total_failed));
    printf("Requests/sec:   %.2f\n", total_reqs / total_duration);

    if (total_reqs == 0) return;

    int p50_target = total_reqs * 0.50;
    int p95_target = total_reqs * 0.95;
    int p99_target = total_reqs * 0.99;
    
    int cumulative = 0;
    double p50 = 0, p95 = 0, p99 = 0;

    for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
        cumulative += global_histogram[i];
        
        if (p50 == 0 && cumulative >= p50_target) p50 = (i * BUCKET_RESOLUTION_US) / 1000.0;
        if (p95 == 0 && cumulative >= p95_target) p95 = (i * BUCKET_RESOLUTION_US) / 1000.0;
        if (p99 == 0 && cumulative >= p99_target) p99 = (i * BUCKET_RESOLUTION_US) / 1000.0;
        
        if (cumulative >= total_reqs) break;
    }

    printf("\nLatency Percentiles:\n");
    printf("p50: %.2f ms\n", p50);
    printf("p95: %.2f ms\n", p95);
    printf("p99: %.2f ms\n", p99);
}

int main(int argc, char *argv[]) {
    maximize_fd_limit();
    signal(SIGINT, handle_sigint);
    char *host = NULL;
    char *port = "80";
    char *path = "/";
    int concurrency = 1;
    int threads = 1;
    int duration = 10;
    bool chaos = false;
    int opt;

    while ((opt = getopt(argc, argv, "h:p:P:c:t:d:x")) != -1) {
        switch (opt) {
            case 'h': host = optarg; break;
            case 'p': port = optarg; break;
            case 'P': path = optarg; break;
            case 'c': concurrency = atoi(optarg); break;
            case 't': threads = atoi(optarg); break;
            case 'd': duration = atoi(optarg); break;
            case 'x': chaos = true; break;
            default:
                fprintf(stderr, "Usage: %s -h host [-p port] [-P path] [-c conn] [-t threads] [-d secs] [-x chaos]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (!host || concurrency <= 0 || threads <= 0 || duration <= 0) exit(EXIT_FAILURE);

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port, &hints, &res) != 0) exit(EXIT_FAILURE);

    char request[1024];
    int req_len = snprintf(request, sizeof(request),
             "GET %s HTTP/1.1\r\n"
             "Host: %s\r\n"
             "Connection: close\r\n"
             "\r\n", path, host);

    if (req_len >= (int)sizeof(request)) exit(EXIT_FAILURE);

    pthread_t *thread_pool = calloc(threads, sizeof(pthread_t));
    ThreadContext *contexts = calloc(threads, sizeof(ThreadContext));

    int base_concurrency = concurrency / threads;
    int remainder = concurrency % threads;

    for (int i = 0; i < threads; i++) {
        contexts[i].thread_id = i;
        contexts[i].concurrency = base_concurrency + (i < remainder ? 1 : 0);
        contexts[i].active_connections = contexts[i].concurrency;
        contexts[i].res = res;
        contexts[i].request = request;
        contexts[i].req_len = req_len;
        contexts[i].chaos_mode = chaos;
        contexts[i].prng_seed = (unsigned int)(time(NULL) ^ i);
        memset(contexts[i].histogram, 0, sizeof(contexts[i].histogram));
        
        contexts[i].epoll_fd = epoll_create1(0);
        if (contexts[i].epoll_fd == -1) exit(EXIT_FAILURE);

        contexts[i].connections = calloc(contexts[i].concurrency, sizeof(Connection));
        pthread_create(&thread_pool[i], NULL, worker_thread, &contexts[i]);
    }

    uint64_t start_time = get_time_us();
    uint64_t end_time = start_time + (duration * 1000000ULL);

    while (get_time_us() < end_time) {
        uint64_t now = get_time_us();
        double elapsed = (now - start_time) / 1000000.0;
        double progress = elapsed / duration;
        
        int completed = atomic_load(&total_completed);
        double req_per_sec = completed / (elapsed > 0 ? elapsed : 1);
        
        int bar_width = 40;
        int pos = bar_width * progress;
        
        printf("\r[");
        for (int i = 0; i < bar_width; ++i) {
            if (i < pos) printf("=");
            else if (i == pos) printf(">");
            else printf(" ");
        }
        printf("] %3d%% | Rps: %7.0f | Done: %d", (int)(progress * 100.0), req_per_sec, completed);
        fflush(stdout);
        
        usleep(100000); 
    }

    atomic_store(&keep_running, false);

    for (int i = 0; i < threads; i++) {
        pthread_join(thread_pool[i], NULL);
        for (int b = 0; b < HISTOGRAM_BUCKETS; b++) {
            global_histogram[b] += contexts[i].histogram[b];
        }
        free(contexts[i].connections);
        close(contexts[i].epoll_fd);
    }

    freeaddrinfo(res);
    free(thread_pool);
    free(contexts);

    int final_completed = atomic_load(&total_completed);
    double total_duration = (get_time_us() - start_time) / 1000000.0;
    print_report(final_completed, total_duration);

    return 0;
}