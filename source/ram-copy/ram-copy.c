#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <linux/limits.h>
#include <libgen.h>
#include <sys/param.h>
#include <sched.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define MAX_PATHS 1024
#define PROGRESS_UPDATE_MS 500             // Update progress every 500ms
#define RAM_USE_PERCENT 90                 // percent of free RAM to use across threads
#define PER_THREAD_MIN_BUFSIZE (64 * 1024) // 64KB minimum buffer for pathological low-RAM systems

typedef struct {
    char source_path[PATH_MAX];
    char dest_path[PATH_MAX];
    off_t file_size;
    off_t bytes_copied;
    int status; // 0=pending, 1=in_progress, 2=completed, 3=failed
    pthread_mutex_t mutex;
} file_job_t;

typedef struct {
    file_job_t *jobs;
    size_t job_count;
    size_t job_capacity;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;
    size_t next_job;
} job_queue_t;

typedef struct {
    off_t total_bytes;
    off_t copied_bytes;
    size_t total_files;
    size_t completed_files;
    time_t start_time;
    double current_speed;
    pthread_mutex_t mutex;
} progress_info_t;

typedef struct {
    job_queue_t *queue;
    int thread_id;
    volatile int *should_stop;
    volatile int *device_available;
    size_t per_thread_bufsize;
    int cpu_affinity; // -1 means no affinity set
} worker_thread_data_t;

/* Globals */
static job_queue_t g_queue = {0};
static progress_info_t g_progress = {0};
static volatile int g_should_stop = 0;
static volatile int g_device_available = 1;
static char g_dest_base[PATH_MAX] = {0};

/* Prototypes */
void signal_handler(int sig);
int detect_partition(const char *path, char *device, size_t device_size);
int scan_directory(const char *source, const char *dest, job_queue_t *queue);
void *worker_thread(void *arg);
int verify_file_match(const char *source, const char *dest);
void update_progress_display(void);
void save_progress(const char *progress_file);
int load_progress(const char *progress_file);
int interactive_mode(char sources[][PATH_MAX], char *dest);
int parse_command_line(int argc, char *argv[], char sources[][PATH_MAX], char *dest);
int mkdir_p(const char *path, mode_t mode);

/* Implementation */

void signal_handler(int sig) {
    (void)sig;
    fprintf(stderr, "\nReceived stop signal — will stop after current buffer.\n");
    g_should_stop = 1;
}

/* mkdir -p implementation without invoking shell */
int mkdir_p(const char *path, mode_t mode) {
    if (!path || !*path) return -1;
    char tmp[PATH_MAX];
    char *p = NULL;
    size_t len;

    strncpy(tmp, path, sizeof(tmp));
    tmp[sizeof(tmp)-1] = '\0';
    len = strlen(tmp);
    if (len == 0) return -1;
    if (tmp[len - 1] == '/') tmp[len - 1] = '\0';

    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, mode) != 0) {
                if (errno != EEXIST) return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, mode) != 0) {
        if (errno != EEXIST) return -1;
    }
    return 0;
}

/* Detect device for a path using /proc/mounts (best mountpoint match) */
int detect_partition(const char *path, char *device, size_t device_size) {
    FILE *mounts = fopen("/proc/mounts", "r");
    if (!mounts) {
        perror("fopen /proc/mounts");
        return -1;
    }

    char line[1024];
    char best_mount[PATH_MAX] = "";
    char best_device[PATH_MAX] = "";
    size_t best_match_len = 0;

    while (fgets(line, sizeof(line), mounts)) {
        char dev[PATH_MAX], mount_point[PATH_MAX], fs_type[64];
        if (sscanf(line, "%s %s %s", dev, mount_point, fs_type) == 3) {
            size_t mount_len = strlen(mount_point);
            if (mount_len <= strlen(path) && strncmp(path, mount_point, mount_len) == 0) {
                if (mount_len > best_match_len) {
                    best_match_len = mount_len;
                    strncpy(best_mount, mount_point, PATH_MAX-1);
                    best_mount[PATH_MAX-1] = '\0';
                    strncpy(best_device, dev, PATH_MAX-1);
                    best_device[PATH_MAX-1] = '\0';
                }
            }
        }
    }

    fclose(mounts);
    if (best_match_len > 0) {
        if (device && device_size > 0) {
            strncpy(device, best_device, device_size-1);
            device[device_size-1] = '\0';
        }
        return 0;
    }
    return -1;
}

/* Verify if destination file matches source (quick checks) */
int verify_file_match(const char *source, const char *dest) {
    struct stat src_stat, dest_stat;
    if (stat(dest, &dest_stat) != 0) return -1; // no dest
    if (stat(source, &src_stat) != 0) return -1; // cannot stat source

    if (src_stat.st_size != dest_stat.st_size) return -1;
    if (src_stat.st_size == 0) return 0; // both zero-length

    /* small files -> byte-by-byte */
    if (src_stat.st_size < 1024 * 1024) {
        int sfd = open(source, O_RDONLY);
        int dfd = open(dest, O_RDONLY);
        if (sfd < 0 || dfd < 0) {
            if (sfd >= 0) close(sfd);
            if (dfd >= 0) close(dfd);
            return -1;
        }
        char a[4096], b[4096];
        ssize_t ra, rb;
        while ((ra = read(sfd, a, sizeof(a))) > 0) {
            rb = read(dfd, b, ra);
            if (rb != ra || memcmp(a, b, ra) != 0) {
                close(sfd); close(dfd);
                return -1;
            }
        }
        close(sfd); close(dfd);
        return 0;
    }

    /* Large files: fall back to size-only heuristic */
    return 0;
}

/* Save progress stub (writes minimal state) */
void save_progress(const char *progress_file) {
    FILE *fp = fopen(progress_file, "w");
    if (!fp) return;
    pthread_mutex_lock(&g_queue.queue_mutex);
    fprintf(fp, "%zu\n", g_queue.job_count);
    for (size_t i = 0; i < g_queue.job_count; ++i) {
        file_job_t *job = &g_queue.jobs[i];
        fprintf(fp, "%s|%s|%ld|%ld|%d\n",
                job->source_path, job->dest_path,
                (long)job->file_size, (long)job->bytes_copied, job->status);
    }
    pthread_mutex_unlock(&g_queue.queue_mutex);
    fclose(fp);
}

/* Load progress stub */
int load_progress(const char *progress_file) {
    /* Not implemented for full restore; placeholder */
    (void)progress_file;
    return 0;
}

/* Update progress display */
void update_progress_display(void) {
    pthread_mutex_lock(&g_progress.mutex);
    time_t now = time(NULL);
    double elapsed = difftime(now, g_progress.start_time);
    if (elapsed <= 0) elapsed = 1.0;
    if (g_progress.copied_bytes >= 0) {
        g_progress.current_speed = (double)g_progress.copied_bytes / elapsed;
    }
    double percent = g_progress.total_bytes > 0 ? (double)g_progress.copied_bytes * 100.0 / (double)g_progress.total_bytes : 0.0;
    int bar_width = 50;
    int filled = (int)(percent * bar_width / 100.0);
    printf("\r[");
    for (int i = 0; i < bar_width; ++i) {
        if (i < filled) putchar('=');
        else if (i == filled) putchar('>');
        else putchar(' ');
    }
    printf("] %.1f%% | %zu/%zu files | %.2f MB/s",
           percent,
           g_progress.completed_files,
           g_progress.total_files,
           g_progress.current_speed / (1024.0 * 1024.0));
    fflush(stdout);
    pthread_mutex_unlock(&g_progress.mutex);
}

/* Recursive scan building jobs into queue */
int scan_directory(const char *source, const char *dest, job_queue_t *queue) {
    static int recursion_depth = 0;
    if (recursion_depth > 50) {
        fprintf(stderr, "Max recursion reached at %s\n", source);
        return -1;
    }
    recursion_depth++;
    DIR *d = opendir(source);
    if (!d) {
        fprintf(stderr, "Cannot open dir %s: %s\n", source, strerror(errno));
        recursion_depth--;
        return -1;
    }
    struct dirent *ent;
    int file_count = 0;
    int skipped_count = 0;

    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        char src_path[PATH_MAX];
        char dst_path[PATH_MAX];
        snprintf(src_path, sizeof(src_path), "%s/%s", source, ent->d_name);
        snprintf(dst_path, sizeof(dst_path), "%s/%s", dest, ent->d_name);

        struct stat st;
        if (stat(src_path, &st) != 0) {
            fprintf(stderr, "Cannot stat %s: %s\n", src_path, strerror(errno));
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            if (mkdir_p(dst_path, st.st_mode) != 0) {
                // not fatal
            }
            scan_directory(src_path, dst_path, queue);
        } else if (S_ISREG(st.st_mode)) {
            ++file_count;
            if (verify_file_match(src_path, dst_path) == 0) {
                ++skipped_count;
                if (skipped_count <= 5) {
                    printf("  Skipping (exists): %s\n", src_path);
                }
                pthread_mutex_lock(&g_progress.mutex);
                g_progress.completed_files++;
                g_progress.copied_bytes += st.st_size;
                g_progress.total_files++;
                g_progress.total_bytes += st.st_size;
                pthread_mutex_unlock(&g_progress.mutex);
                continue;
            }
            if ((file_count - skipped_count) <= 5) {
                printf("  Found file: %s (%ld bytes)\n", ent->d_name, (long)st.st_size);
            } else if ((file_count - skipped_count) == 6) {
                printf("  ... (showing only first 5 new files per directory)\n");
            }

            pthread_mutex_lock(&queue->queue_mutex);
            if (queue->job_count >= queue->job_capacity) {
                size_t newcap = queue->job_capacity ? queue->job_capacity * 2 : 1024;
                file_job_t *re = realloc(queue->jobs, newcap * sizeof(file_job_t));
                if (!re) {
                    fprintf(stderr, "Failed to expand job queue\n");
                    pthread_mutex_unlock(&queue->queue_mutex);
                    closedir(d);
                    recursion_depth--;
                    return -1;
                }
                queue->jobs = re;
                queue->job_capacity = newcap;
                printf("  Expanded job queue to %zu\n", newcap);
            }
            file_job_t *job = &queue->jobs[queue->job_count];
            strncpy(job->source_path, src_path, PATH_MAX-1);
            job->source_path[PATH_MAX-1] = '\0';
            strncpy(job->dest_path, dst_path, PATH_MAX-1);
            job->dest_path[PATH_MAX-1] = '\0';
            job->file_size = st.st_size;
            job->bytes_copied = 0;
            job->status = 0;
            pthread_mutex_init(&job->mutex, NULL);
            queue->job_count++;
            pthread_mutex_unlock(&queue->queue_mutex);

            pthread_mutex_lock(&g_progress.mutex);
            g_progress.total_files++;
            g_progress.total_bytes += st.st_size;
            pthread_mutex_unlock(&g_progress.mutex);
        }
    }

    closedir(d);
    recursion_depth--;
    printf("  Completed directory: %s (%d new files, %d already exist)\n", source, file_count - skipped_count, skipped_count);
    return 0;
}

/* Worker thread — reads/writes using the full per-thread buffer without artificial caps */
void *worker_thread(void *arg) {
    worker_thread_data_t *data = (worker_thread_data_t *)arg;
    size_t bufsize = data->per_thread_bufsize;
    if (bufsize < PER_THREAD_MIN_BUFSIZE) bufsize = PER_THREAD_MIN_BUFSIZE;

    char *thread_buffer = malloc(bufsize);
    if (!thread_buffer) {
        fprintf(stderr, "Thread %d: failed to allocate thread buffer of %zu bytes\n", data->thread_id, bufsize);
        return NULL;
    }

    /* optional: set CPU affinity if requested */
#ifdef __linux__
    if (data->cpu_affinity >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(data->cpu_affinity, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }
#endif

    printf("Worker thread %d started (buf=%zu bytes, affinity=%d)\n", data->thread_id, bufsize, data->cpu_affinity);

    while (!*data->should_stop) {
        size_t job_index = SIZE_MAX;

        /* Atomically get next job index */
        pthread_mutex_lock(&data->queue->queue_mutex);
        if (data->queue->next_job < data->queue->job_count) {
            job_index = data->queue->next_job++;
            data->queue->jobs[job_index].status = 1; // in progress
        }
        pthread_mutex_unlock(&data->queue->queue_mutex);

        if (job_index == SIZE_MAX) {
            /* No available job — exit loop */
            break;
        }

        file_job_t *job = &data->queue->jobs[job_index];
        printf("Thread %d: processing %s -> %s\n", data->thread_id, job->source_path, job->dest_path);

        /* If destination already matches, mark done */
        if (verify_file_match(job->source_path, job->dest_path) == 0) {
            job->status = 2;
            job->bytes_copied = job->file_size;
            pthread_mutex_lock(&g_progress.mutex);
            g_progress.completed_files++;
            g_progress.copied_bytes += job->file_size;
            pthread_mutex_unlock(&g_progress.mutex);
            continue;
        }

        /* Ensure destination directory exists (safe) */
        char dest_dir[PATH_MAX];
        strncpy(dest_dir, job->dest_path, PATH_MAX-1);
        dest_dir[PATH_MAX-1] = '\0';
        char *slash = strrchr(dest_dir, '/');
        if (slash) {
            *slash = '\0';
            if (mkdir_p(dest_dir, 0755) != 0) {
                fprintf(stderr, "Thread %d: mkdir_p failed for %s (%s)\n", data->thread_id, dest_dir, strerror(errno));
            }
        }

        /* Open source */
        int src_fd = open(job->source_path, O_RDONLY | O_LARGEFILE);
        if (src_fd < 0) {
            fprintf(stderr, "Thread %d: open source %s failed: %s\n", data->thread_id, job->source_path, strerror(errno));
            job->status = 3;
            continue;
        }

        /* Open temp dest (partial) */
        char temp_dest[PATH_MAX];
        snprintf(temp_dest, sizeof(temp_dest), "%s.partial", job->dest_path);
        int dest_fd = open(temp_dest, O_WRONLY | O_CREAT | O_LARGEFILE, 0644);
        if (dest_fd < 0) {
            fprintf(stderr, "Thread %d: open dest %s failed: %s\n", data->thread_id, temp_dest, strerror(errno));
            close(src_fd);
            job->status = 3;
            continue;
        }

        /* Resume if partial exists */
        off_t existing = lseek(dest_fd, 0, SEEK_END);
        if (existing > 0) {
            job->bytes_copied = existing;
            if (lseek(src_fd, existing, SEEK_SET) < 0) {
                fprintf(stderr, "Thread %d: lseek source failed\n", data->thread_id);
                close(src_fd); close(dest_fd);
                job->status = 3;
                continue;
            }
        }

        /* Hints to kernel for sequential read/write */
#ifdef POSIX_FADV_SEQUENTIAL
        posix_fadvise(src_fd, job->bytes_copied, 0, POSIX_FADV_SEQUENTIAL);
#endif
#ifdef POSIX_FADV_WILLNEED
        posix_fadvise(dest_fd, 0, 0, POSIX_FADV_WILLNEED);
#endif

        /* Copy loop — use full bufsize each iteration (or remaining file bytes) */
        while (job->bytes_copied < job->file_size && !*data->should_stop) {
            off_t remaining = job->file_size - job->bytes_copied;
            size_t to_read = (remaining > (off_t)bufsize) ? bufsize : (size_t)remaining;

            ssize_t rr = read(src_fd, thread_buffer, to_read);
            if (rr < 0) {
                fprintf(stderr, "Thread %d: read error on %s: %s\n", data->thread_id, job->source_path, strerror(errno));
                break;
            } else if (rr == 0) {
                /* Unexpected EOF */
                fprintf(stderr, "Thread %d: unexpected EOF in source %s\n", data->thread_id, job->source_path);
                break;
            }

            ssize_t written_total = 0;
            while (written_total < rr) {
                ssize_t ww = write(dest_fd, thread_buffer + written_total, rr - written_total);
                if (ww < 0) {
                    fprintf(stderr, "Thread %d: write error to %s: %s\n", data->thread_id, temp_dest, strerror(errno));
                    break;
                }
                written_total += ww;
            }
            if (written_total != rr) {
                /* write failed */
                break;
            }

            job->bytes_copied += written_total;

            pthread_mutex_lock(&g_progress.mutex);
            g_progress.copied_bytes += written_total;
            pthread_mutex_unlock(&g_progress.mutex);
        }

        close(src_fd);
        close(dest_fd);

        if (job->bytes_copied >= job->file_size) {
            /* move partial -> final */
            if (rename(temp_dest, job->dest_path) == 0) {
                job->status = 2;
                pthread_mutex_lock(&g_progress.mutex);
                g_progress.completed_files++;
                pthread_mutex_unlock(&g_progress.mutex);
                printf("Thread %d: Completed %s\n", data->thread_id, job->dest_path);
            } else {
                fprintf(stderr, "Thread %d: rename failed %s -> %s: %s\n", data->thread_id, temp_dest, job->dest_path, strerror(errno));
                job->status = 3;
            }
        } else {
            fprintf(stderr, "Thread %d: incomplete %s (%ld/%ld)\n", data->thread_id, job->source_path, (long)job->bytes_copied, (long)job->file_size);
            job->status = 3;
        }
    } /* end worker loop */

    free(thread_buffer);
    printf("Worker thread %d finished\n", data->thread_id);
    return NULL;
}

/* Interactive mode prompt */
int interactive_mode(char sources[][PATH_MAX], char *dest) {
    printf("Interactive Mode\n");
    printf("Enter source directory: ");
    if (!fgets(sources[0], PATH_MAX, stdin)) return 0;
    sources[0][strcspn(sources[0], "\n")] = '\0';
    printf("Enter destination directory: ");
    if (!fgets(dest, PATH_MAX, stdin)) return 0;
    dest[strcspn(dest, "\n")] = '\0';
    return 1;
}

/* Command-line parser: last arg is destination, others are sources */
int parse_command_line(int argc, char *argv[], char sources[][PATH_MAX], char *dest) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s SOURCE... DEST\n", argc?argv[0]:"ram-copy");
        return -1;
    }
    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        fprintf(stderr, "Usage: %s SOURCE... DEST\n", argv[0]);
        return -1;
    }
    int last = argc - 1;
    if (strlen(argv[last]) >= PATH_MAX) {
        fprintf(stderr, "Destination path too long\n");
        return -1;
    }
    strncpy(dest, argv[last], PATH_MAX-1);
    dest[PATH_MAX-1] = '\0';
    int sc = 0;
    for (int i = 1; i < last && sc < MAX_PATHS; ++i) {
        if (strlen(argv[i]) >= PATH_MAX) {
            fprintf(stderr, "Skipping too-long source: %s\n", argv[i]);
            continue;
        }
        strncpy(sources[sc++], argv[i], PATH_MAX-1);
        sources[sc-1][PATH_MAX-1] = '\0';
    }
    if (sc == 0) {
        fprintf(stderr, "No sources provided\n");
        return -1;
    }
    return sc;
}

/* main */
int main(int argc, char *argv[]) {
    char sources[MAX_PATHS][PATH_MAX];
    char dest[PATH_MAX];
    int source_count = 0;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    pthread_mutex_init(&g_queue.queue_mutex, NULL);
    pthread_cond_init(&g_queue.queue_cond, NULL);
    pthread_mutex_init(&g_progress.mutex, NULL);

    printf("RAM Copy - Hardware-maximized File Recovery Tool\n");
    printf("================================================\n\n");

    if (argc < 2) {
        source_count = interactive_mode(sources, dest);
        if (source_count <= 0) {
            fprintf(stderr, "No source provided\n");
            return 1;
        }
    } else {
        source_count = parse_command_line(argc, argv, sources, dest);
        if (source_count <= 0) return 1;
    }

    /* init queue */
    g_queue.job_capacity = 8192;
    g_queue.job_count = 0;
    g_queue.jobs = calloc(g_queue.job_capacity, sizeof(file_job_t));
    if (!g_queue.jobs) {
        perror("calloc");
        return 1;
    }
    g_queue.next_job = 0;

    strncpy(g_dest_base, dest, PATH_MAX-1);
    g_dest_base[PATH_MAX-1] = '\0';

    /* scan */
    printf("Scanning directories...\n");
    for (int i = 0; i < source_count; ++i) {
        printf("Checking source: %s\n", sources[i]);
        struct stat st;
        if (stat(sources[i], &st) != 0) {
            fprintf(stderr, "Cannot access source %s: %s\n", sources[i], strerror(errno));
            continue;
        }
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Source not a directory: %s\n", sources[i]);
            continue;
        }
        if (scan_directory(sources[i], dest, &g_queue) != 0) {
            fprintf(stderr, "Scan failed for %s\n", sources[i]);
        }
    }

    printf("Directory scanning completed!\n");
    printf("Total files found: %zu\n", g_progress.total_files);
    printf("Files already copied (skipped): %zu\n", g_progress.completed_files);
    printf("Files that need copying: %zu\n", g_queue.job_count);
    printf("Total data: %ld MB, Need to copy: %ld MB\n",
           (long)(g_progress.total_bytes / (1024*1024)),
           (long)((g_progress.total_bytes - g_progress.copied_bytes) / (1024*1024)));

    if (g_queue.job_count == 0) {
        printf("Nothing to copy.\n");
        free(g_queue.jobs);
        return 0;
    }

    /* progress init */
    g_progress.start_time = time(NULL);
    g_progress.copied_bytes = 0;

    load_progress(".ram_copy_progress");

    /* Hardware detection */
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc < 1) nproc = 1;
    /* Slight oversubscribe to keep cores busy */
    size_t num_threads = (size_t)(nproc * 2);

    /* Determine available RAM (free pages) */
    long pagesize = sysconf(_SC_PAGESIZE);
    long free_pages = sysconf(_SC_AVPHYS_PAGES);
    if (pagesize <= 0) pagesize = 4096;
    if (free_pages < 0) free_pages = 0;
    size_t free_ram = (size_t)free_pages * (size_t)pagesize;

    /* Use a percentage of free RAM across threads, leave some for OS */
    size_t use_ram = (free_ram * RAM_USE_PERCENT) / 100;
    if (use_ram == 0) use_ram = PER_THREAD_MIN_BUFSIZE * num_threads;

    size_t per_thread_bufsize = use_ram / (num_threads > 0 ? num_threads : 1);
    if (per_thread_bufsize < PER_THREAD_MIN_BUFSIZE) per_thread_bufsize = PER_THREAD_MIN_BUFSIZE;

    printf("Hardware: CPU cores=%ld, threads=%zu, free_ram=%zu MB, per-thread buf=%zu MB\n",
           nproc, num_threads, free_ram / (1024*1024), per_thread_bufsize / (1024*1024));

    /* Allocate dynamic arrays for threads */
    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
    worker_thread_data_t *tdata = calloc(num_threads, sizeof(worker_thread_data_t));
    if (!threads || !tdata) {
        fprintf(stderr, "Failed to allocate thread structures\n");
        free(g_queue.jobs);
        free(threads); free(tdata);
        return 1;
    }

    printf("Starting %zu worker threads for file transfer...\n", num_threads);

    for (size_t i = 0; i < num_threads; ++i) {
        tdata[i].queue = &g_queue;
        tdata[i].thread_id = (int)i;
        tdata[i].should_stop = &g_should_stop;
        tdata[i].device_available = &g_device_available;
        tdata[i].per_thread_bufsize = per_thread_bufsize;
        /* round-robin CPU affinity: improves locality on many-core systems */
#ifdef __linux__
        tdata[i].cpu_affinity = (int)(i % (size_t)nproc);
#else
        tdata[i].cpu_affinity = -1;
#endif
        if (pthread_create(&threads[i], NULL, worker_thread, &tdata[i]) != 0) {
            fprintf(stderr, "Failed to create thread %zu\n", i);
            g_should_stop = 1;
            num_threads = i;
            break;
        }
    }

    /* monitor progress until all done */
    while (!g_should_stop) {
        pthread_mutex_lock(&g_queue.queue_mutex);
        int done_flag = 1;
        if (g_queue.next_job < g_queue.job_count) done_flag = 0;
        pthread_mutex_unlock(&g_queue.queue_mutex);

        update_progress_display();
        save_progress(".ram_copy_progress");
        if (done_flag && g_progress.completed_files >= g_queue.job_count) break;
        usleep(PROGRESS_UPDATE_MS * 1000);
    }

    /* shutdown */
    g_should_stop = 1;
    pthread_cond_broadcast(&g_queue.queue_cond);
    for (size_t i = 0; i < num_threads; ++i) {
        pthread_join(threads[i], NULL);
    }

    update_progress_display();
    printf("\n\nCopy operation completed!\n");
    printf("Files copied: %zu/%zu\n", g_progress.completed_files, g_queue.job_count);
    printf("Data copied: %ld MB\n", (long)(g_progress.copied_bytes / (1024*1024)));

    unlink(".ram_copy_progress");
    free(g_queue.jobs);
    free(threads);
    free(tdata);

    return 0;
}
