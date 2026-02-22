#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MODEL_COUNT_NO_MAIN
#include "../model_count.c"

static uint64_t get_count(const HashTable *table, const char *model) {
    uint64_t h = hash_str(model);
    size_t idx = (size_t)(h % table->bucket_count);
    const Entry *e = table->buckets[idx];
    while (e) {
        if (strcmp(e->key, model) == 0) {
            return e->count;
        }
        e = e->next;
    }
    return 0;
}

static void write_or_die(FILE *fp, const char *text) {
    if (fputs(text, fp) == EOF) {
        fprintf(stderr, "Failed to write test input\n");
        exit(1);
    }
}

static void expect_counts(const char *json,
                          size_t expected_unique,
                          uint64_t rdv2_count,
                          uint64_t abc_count,
                          uint64_t xyz_count) {
    FILE *fp = tmpfile();
    if (!fp) {
        fprintf(stderr, "tmpfile() failed\n");
        exit(1);
    }

    write_or_die(fp, json);
    if (fflush(fp) != 0 || fseek(fp, 0, SEEK_SET) != 0) {
        fprintf(stderr, "Failed to reset test file\n");
        fclose(fp);
        exit(1);
    }

    HashTable table;
    uint64_t models_seen = 0;
    ProgressState progress = {0};
    progress.start_time = now_seconds();
    progress.last_time = progress.start_time;
    progress.last_models_seen = 0;
    table_init(&table, INITIAL_BUCKETS);

    if (!process_file(fp, &table, &models_seen, &progress, (long)strlen(json))) {
        fprintf(stderr, "process_file() failed for input: %s\n", json);
        table_free(&table);
        fclose(fp);
        exit(1);
    }

    if (table.size != expected_unique) {
        fprintf(stderr, "Expected %zu unique models, got %zu\n", expected_unique, table.size);
        table_free(&table);
        fclose(fp);
        exit(1);
    }
    if (get_count(&table, "RDV2") != rdv2_count) {
        fprintf(stderr, "Expected RDV2=%llu, got %llu\n",
                (unsigned long long)rdv2_count,
                (unsigned long long)get_count(&table, "RDV2"));
        table_free(&table);
        fclose(fp);
        exit(1);
    }
    if (get_count(&table, "ABC") != abc_count) {
        fprintf(stderr, "Expected ABC=%llu, got %llu\n",
                (unsigned long long)abc_count,
                (unsigned long long)get_count(&table, "ABC"));
        table_free(&table);
        fclose(fp);
        exit(1);
    }
    if (get_count(&table, "XYZ") != xyz_count) {
        fprintf(stderr, "Expected XYZ=%llu, got %llu\n",
                (unsigned long long)xyz_count,
                (unsigned long long)get_count(&table, "XYZ"));
        table_free(&table);
        fclose(fp);
        exit(1);
    }

    table_free(&table);
    fclose(fp);
}

int main(void) {
    expect_counts(
        "[{\"id\":1,\"model\":\"RDV2\",\"serial\":\"A\"},"
        "{\"id\":2,\"model\":\"ABC\",\"serial\":\"B\"},"
        "{\"id\":3,\"model\":\"RDV2\",\"serial\":\"C\"}]",
        2, 2, 1, 0);

    expect_counts(
        "[{\"id\":1,\"serial\":\"A\"},"
        "{\"id\":2,\"model\":\"XYZ\",\"serial\":\"B\"},"
        "{\"id\":3,\"nested\":{\"model\":\"RDV2\"},\"serial\":\"C\"}]",
        1, 0, 0, 1);

    expect_counts(
        "[{\"model\":\"RDV2\"},{\"model\":123},{\"model\":\"RDV2\"},{\"model\":\"ABC\"}]",
        2, 2, 1, 0);

    printf("All unit tests passed.\n");
    return 0;
}
