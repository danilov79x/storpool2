#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define KEY_MODEL "model"
#define INITIAL_BUCKETS 4096
#define LOAD_FACTOR_NUM 3
#define LOAD_FACTOR_DEN 4
#define PROGRESS_INTERVAL_SEC 5.0

typedef struct Entry {
    char *key;
    uint64_t count;
    struct Entry *next;
} Entry;

typedef struct {
    Entry **buckets;
    size_t bucket_count;
    size_t size;
} HashTable;

static uint64_t hash_str(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static void die(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

static void *xmalloc(size_t n) {
    void *p = malloc(n);
    if (!p) {
        die("Out of memory");
    }
    return p;
}

static char *xstrdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *d = (char *)xmalloc(n);
    memcpy(d, s, n);
    return d;
}

static void table_init(HashTable *t, size_t buckets) {
    t->bucket_count = buckets;
    t->size = 0;
    t->buckets = (Entry **)calloc(buckets, sizeof(Entry *));
    if (!t->buckets) {
        die("Out of memory");
    }
}

static void table_free(HashTable *t) {
    for (size_t i = 0; i < t->bucket_count; ++i) {
        Entry *e = t->buckets[i];
        while (e) {
            Entry *next = e->next;
            free(e->key);
            free(e);
            e = next;
        }
    }
    free(t->buckets);
}

static void table_rehash(HashTable *t) {
    size_t new_count = t->bucket_count * 2;
    Entry **new_buckets = (Entry **)calloc(new_count, sizeof(Entry *));
    if (!new_buckets) {
        die("Out of memory");
    }

    for (size_t i = 0; i < t->bucket_count; ++i) {
        Entry *e = t->buckets[i];
        while (e) {
            Entry *next = e->next;
            uint64_t h = hash_str(e->key);
            size_t idx = (size_t)(h % new_count);
            e->next = new_buckets[idx];
            new_buckets[idx] = e;
            e = next;
        }
    }

    free(t->buckets);
    t->buckets = new_buckets;
    t->bucket_count = new_count;
}

static void table_inc(HashTable *t, const char *key) {
    if ((t->size * LOAD_FACTOR_DEN) >= (t->bucket_count * LOAD_FACTOR_NUM)) {
        table_rehash(t);
    }

    uint64_t h = hash_str(key);
    size_t idx = (size_t)(h % t->bucket_count);
    Entry *e = t->buckets[idx];
    while (e) {
        if (strcmp(e->key, key) == 0) {
            e->count++;
            return;
        }
        e = e->next;
    }

    Entry *n = (Entry *)xmalloc(sizeof(Entry));
    n->key = xstrdup(key);
    n->count = 1;
    n->next = t->buckets[idx];
    t->buckets[idx] = n;
    t->size++;
}

static int skip_ws(FILE *fp) {
    int c;
    do {
        c = fgetc(fp);
    } while (c != EOF && isspace((unsigned char)c));
    return c;
}

static char *read_json_string(FILE *fp) {
    size_t cap = 32;
    size_t len = 0;
    char *buf = (char *)xmalloc(cap);

    for (;;) {
        int c = fgetc(fp);
        if (c == EOF) {
            free(buf);
            return NULL;
        }
        if (c == '"') {
            buf[len] = '\0';
            return buf;
        }
        if (c == '\\') {
            int esc = fgetc(fp);
            if (esc == EOF) {
                free(buf);
                return NULL;
            }
            if (esc == 'u') {
                for (int i = 0; i < 4; ++i) {
                    int h = fgetc(fp);
                    if (h == EOF || !isxdigit((unsigned char)h)) {
                        free(buf);
                        return NULL;
                    }
                }
                c = '?';
            } else {
                switch (esc) {
                    case '"': c = '"'; break;
                    case '\\': c = '\\'; break;
                    case '/': c = '/'; break;
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    default: c = esc; break;
                }
            }
        }

        if (len + 1 >= cap) {
            cap *= 2;
            char *tmp = (char *)realloc(buf, cap);
            if (!tmp) {
                free(buf);
                die("Out of memory");
            }
            buf = tmp;
        }
        buf[len++] = (char)c;
    }
}

static int consume_json_value(FILE *fp, int first) {
    int c = first;

    if (c == '"') {
        char *s = read_json_string(fp);
        if (!s) return 0;
        free(s);
        return 1;
    }

    if (c == '{' || c == '[') {
        int depth = 1;
        int in_string = 0;
        int esc = 0;
        while (depth > 0) {
            c = fgetc(fp);
            if (c == EOF) return 0;

            if (in_string) {
                if (esc) {
                    esc = 0;
                } else if (c == '\\') {
                    esc = 1;
                } else if (c == '"') {
                    in_string = 0;
                }
                continue;
            }

            if (c == '"') {
                in_string = 1;
            } else if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
            }
        }
        return 1;
    }

    while (c != EOF && c != ',' && c != '}' && c != ']' && !isspace((unsigned char)c)) {
        c = fgetc(fp);
    }
    if (c == ',' || c == '}' || c == ']') {
        ungetc(c, fp);
    }
    return 1;
}

typedef struct {
    double start_time;
    double last_time;
    uint64_t last_models_seen;
} ProgressState;

static double now_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void print_progress_with_pct(FILE *fp, uint64_t models_seen, size_t unique_models,
                                    ProgressState *progress, long total_bytes) {
    long pos = ftell(fp);
    double pct = 0.0;
    if (total_bytes > 0 && pos >= 0) {
        pct = 100.0 * (double)pos / (double)total_bytes;
        if (pct > 100.0) pct = 100.0;
    }

    FILE *mem = fopen("/proc/self/statm", "r");
    if (!mem) {
        return;
    }

    unsigned long size_pages = 0;
    unsigned long rss_pages = 0;
    if (fscanf(mem, "%lu %lu", &size_pages, &rss_pages) == 2) {
        long page_size = sysconf(_SC_PAGESIZE);
        if (page_size > 0) {
            double t = now_seconds();
            double elapsed_total = t - progress->start_time;
            double elapsed_interval = t - progress->last_time;
            (void)elapsed_total;
            double interval_speed = elapsed_interval > 0.0
                ? (double)(models_seen - progress->last_models_seen) / elapsed_interval
                : 0.0;
            double rss_mb = (double)rss_pages * (double)page_size / (1024.0 * 1024.0);
            fprintf(stderr,
                    "\r%.2f%% processed, %llu models, unique %zu, RSS %.2f MB, speed %.0f models/s",
                    pct, (unsigned long long)models_seen, unique_models, rss_mb, interval_speed);
            fflush(stderr);
            progress->last_time = t;
            progress->last_models_seen = models_seen;
        }
    }

    fclose(mem);
}

static int process_file(FILE *fp, HashTable *table, uint64_t *models_seen, ProgressState *progress, long total_bytes) {
    int c;
    while ((c = fgetc(fp)) != EOF) {
        if (c != '"') continue;

        char *key = read_json_string(fp);
        if (!key) return 0;

        c = skip_ws(fp);
        if (c != ':') {
            free(key);
            continue;
        }

        c = skip_ws(fp);
        if (c == EOF) {
            free(key);
            break;
        }

        if (strcmp(key, KEY_MODEL) == 0 && c == '"') {
            char *val = read_json_string(fp);
            if (!val) {
                free(key);
                return 0;
            }
            table_inc(table, val);
            (*models_seen)++;
            if ((now_seconds() - progress->last_time) >= PROGRESS_INTERVAL_SEC) {
                print_progress_with_pct(fp, *models_seen, table->size, progress, total_bytes);
            }
            free(val);
        } else {
            if (!consume_json_value(fp, c)) {
                free(key);
                return 0;
            }
        }

        free(key);
    }

    return 1;
}

typedef struct {
    const char *key;
    uint64_t count;
} Pair;

#ifndef MODEL_COUNT_NO_MAIN
static int pair_cmp(const void *a, const void *b) {
    const Pair *pa = (const Pair *)a;
    const Pair *pb = (const Pair *)b;
    if (pa->count < pb->count) return 1;
    if (pa->count > pb->count) return -1;
    return strcmp(pa->key, pb->key);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file.json>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *path = argv[1];
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "Cannot open '%s': %s\n", path, strerror(errno));
        return EXIT_FAILURE;
    }

    HashTable table;
    uint64_t models_seen = 0;
    long total_bytes = 0;
    ProgressState progress = {0};
    progress.start_time = now_seconds();
    progress.last_time = progress.start_time;
    progress.last_models_seen = 0;
    table_init(&table, INITIAL_BUCKETS);

    if (fseek(fp, 0, SEEK_END) == 0) {
        long end = ftell(fp);
        if (end > 0) total_bytes = end;
        fseek(fp, 0, SEEK_SET);
    }

    if (!process_file(fp, &table, &models_seen, &progress, total_bytes)) {
        fprintf(stderr, "Parse error while reading '%s'\n", path);
        fclose(fp);
        table_free(&table);
        return EXIT_FAILURE;
    }

    if (progress.last_models_seen > 0) {
        fputc('\n', stderr);
    }

    fclose(fp);

    Pair *pairs = (Pair *)xmalloc(table.size * sizeof(Pair));
    size_t idx = 0;
    for (size_t i = 0; i < table.bucket_count; ++i) {
        for (Entry *e = table.buckets[i]; e; e = e->next) {
            pairs[idx].key = e->key;
            pairs[idx].count = e->count;
            idx++;
        }
    }

    qsort(pairs, table.size, sizeof(Pair), pair_cmp);

    printf("Unique models: %zu\n", table.size);
    for (size_t i = 0; i < table.size; ++i) {
        printf("%s: %llu\n", pairs[i].key, (unsigned long long)pairs[i].count);
    }

    free(pairs);
    table_free(&table);
    return EXIT_SUCCESS;
}
#endif
