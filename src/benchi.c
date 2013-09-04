/*
* Copyright (c) 2013 Spotify AB
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>

static void _errno_assert(const char *file, int line, int i) {
  if (i != 0) {
    printf("%s:%d: assertion failed: %s\n", file, line, strerror(errno));
    exit(i);
  }
}

#define errno_assert(i) _errno_assert(__FILE__, __LINE__, i)

static void rm_rec(const char *dir) {
  DIR *tmpd = opendir(dir);
  if (tmpd != NULL) {
    struct dirent *d;
    while ((d = readdir(tmpd))) {
      char subdir[100];
      sprintf(subdir, "%s/%s", dir, d->d_name);
      if(strcmp(d->d_name, ".") &&
         strcmp(d->d_name, "..")) {
        if (d->d_type == DT_DIR) {
          rm_rec(subdir);
        } else if (d->d_type == DT_REG) {
          remove(subdir);
        }
      }
    }
    closedir(tmpd);
  }
  remove(dir);
}

static void rm_all_rec(const char** files) {
  int i = 0;
  while (1) {
    const char *filename = files[i];
    if (filename == NULL) {
      return;
    }
    rm_rec(filename);
    i++;
  }
}


static size_t file_size_rec(const char *dir) {
  struct stat buf;
  errno_assert(stat(dir, &buf));
  if (S_ISREG(buf.st_mode)) {
    return buf.st_size;
  } else if (S_ISDIR(buf.st_mode)) {
    size_t sum = 0;
    DIR *tmpd = opendir(dir);
    if (tmpd != NULL) {
      struct dirent *d;
      while ((d = readdir(tmpd))) {
        char subdir[100];
        sprintf(subdir, "%s/%s", dir, d->d_name);
        if(strcmp(d->d_name, ".") &&
           strcmp(d->d_name, "..")) {
          sum += file_size_rec(subdir);
        }
      }
      closedir(tmpd);
    }
    return sum;
  } else {
    return 0;
  }
}

static size_t total_file_size(const char** files) {
  size_t sum = 0;
  int i = 0;
  while (1) {
    const char *filename = files[i];
    if (filename == NULL) {
      return sum;
    }
    sum += file_size_rec(filename);
    i++;
  }
}

#include <sys/time.h>
#include <sys/resource.h>
static void wall(struct timeval *tv)
{
	gettimeofday(tv, NULL);
}

static void cpu(struct timeval *tv)
{
	struct rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	tv[0] = ru.ru_utime;
	tv[1] = ru.ru_stime;
}

/* tv1 = tv2 - tv1 */
static void timesub(struct timeval *tv1, struct timeval *tv2)
{
	tv1->tv_usec = tv2->tv_usec - tv1->tv_usec;
	if (tv1->tv_usec < 0) {
		tv1->tv_usec += 1000000;
		tv1->tv_sec++;
	}
	tv1->tv_sec = tv2->tv_sec - tv1->tv_sec;
}

typedef struct {
  char *name;
  void (*create)(int n);
  void (*randomaccess)(int n, int lookups);
  const char** (*files)();
} candidate;

/* Sparkey stuff */

#include "sparkey.h"

static void _sparkey_assert(const char *file, int line, sparkey_returncode i) {
  if (i != SPARKEY_SUCCESS) {
    printf("%s:%d: assertion failed: %s\n", file, line, sparkey_errstring(i));
    exit(i);
  }
}

#define sparkey_assert(i) _sparkey_assert(__FILE__, __LINE__, i)


static void sparkey_create(int n, sparkey_compression_type compression_type, int block_size) {
  sparkey_logwriter *mywriter;
  sparkey_assert(sparkey_logwriter_create(&mywriter, "test.spl", compression_type, block_size));
  for (int i = 0; i < n; i++) {
    char myvalue[100];
    sprintf(myvalue, "value_%d", i);
    sparkey_assert(sparkey_logwriter_put(mywriter, sizeof(i), (uint8_t*)&i, strlen(myvalue), (uint8_t*)myvalue));
  }
  sparkey_assert(sparkey_logwriter_close(&mywriter));
  sparkey_assert(sparkey_hash_write("test.spi", "test.spl", 0));
}

static void sparkey_randomaccess(int n, int lookups) {
  sparkey_hashreader *myreader;
  sparkey_logiter *myiter;
  sparkey_assert(sparkey_hash_open(&myreader, "test.spi", "test.spl"));
  sparkey_logreader *logreader = sparkey_hash_getreader(myreader);
  sparkey_assert(sparkey_logiter_create(&myiter, logreader));

  uint8_t *valuebuf = malloc(sparkey_logreader_maxvaluelen(logreader));

  for (int i = 0; i < lookups; i++) {
    char myvalue[100];
    int r = rand() % n;
    sprintf(myvalue, "value_%d", r);
    sparkey_assert(sparkey_hash_get(myreader, (uint8_t*)&r, sizeof(r), myiter));
    if (sparkey_logiter_state(myiter) != SPARKEY_ITER_ACTIVE) {
      printf("Failed to lookup key: %d\n", r);
      exit(1);
    }

    uint64_t wanted_valuelen = sparkey_logiter_valuelen(myiter);
    uint64_t actual_valuelen;
    sparkey_assert(sparkey_logiter_fill_value(myiter, logreader, wanted_valuelen, valuebuf, &actual_valuelen));
    if (actual_valuelen != strlen(myvalue) || memcmp(myvalue, valuebuf, actual_valuelen)) {
      printf("Did not get the expected value for key: %d\n", r);
      exit(1);
    }
  }
  sparkey_logiter_close(&myiter);
  sparkey_hash_close(&myreader);
}

static void sparkey_create_uncompressed(int n) {
  sparkey_create(n, SPARKEY_COMPRESSION_NONE, 0);
}

static void sparkey_create_compressed(int n) {
  sparkey_create(n, SPARKEY_COMPRESSION_SNAPPY, 1024);
}

static const char* sparkey_list[] = {"test.spi", "test.spl", NULL};

static const char** sparkey_files() {
  return sparkey_list;
}

static candidate sparkey_candidate_uncompressed = {
  "Sparkey uncompressed", &sparkey_create_uncompressed, &sparkey_randomaccess, &sparkey_files
};

static candidate sparkey_candidate_compressed = {
  "Sparkey compressed(1024)", &sparkey_create_compressed, &sparkey_randomaccess, &sparkey_files
};

/* main */

void test(candidate *c, int n, int lookups) {
  struct timeval t1_wall, t2_wall, t3_wall;
  struct timeval t1_cpu[2], t2_cpu[2], t3_cpu[2];
  float f;

  printf("Testing bulk insert of %d elements and %d random lookups\n", n, lookups);

  printf("  Candidate: %s\n", c->name);
  rm_all_rec(c->files());

  wall(&t1_wall);
  cpu(t1_cpu);

  c->create(n);

  wall(&t2_wall);
  cpu(t2_cpu);
  timesub(&t1_wall, &t2_wall);
  timesub(&t1_cpu[0], &t2_cpu[0]);
  timesub(&t1_cpu[1], &t2_cpu[1]);

  printf("    creation time (wall):     %d.%06d\n", (int)t1_wall.tv_sec, (int)t1_wall.tv_usec);
  printf("    creation time (ucpu):     %d.%06d\n", (int)t1_cpu[0].tv_sec, (int)t1_cpu[0].tv_usec);
  printf("    creation time (scpu):     %d.%06d\n", (int)t1_cpu[1].tv_sec, (int)t1_cpu[1].tv_usec);
  if (!t1_cpu[0].tv_sec && !t1_cpu[0].tv_usec) {
    f = t1_wall.tv_sec + 1e-6 * t1_wall.tv_usec;
  } else {
    f =  t1_cpu[0].tv_sec + 1e-6 * t1_cpu[0].tv_usec;
    f += t1_cpu[1].tv_sec + 1e-6 * t1_cpu[1].tv_usec;
  }
  printf("    throughput (puts/cpusec): %2.2f\n", (float) n / f);
  printf("    file size:                %zu\n", total_file_size(c->files()));

  c->randomaccess(n, lookups);

  wall(&t3_wall);
  cpu(t3_cpu);
  timesub(&t2_wall, &t3_wall);
  timesub(&t2_cpu[0], &t3_cpu[0]);
  timesub(&t2_cpu[1], &t3_cpu[1]);

  printf("    lookup time (wall):          %d.%06d\n", (int)t2_wall.tv_sec, (int)t2_wall.tv_usec);
  printf("    lookup time (ucpu):          %d.%06d\n", (int)t2_cpu[0].tv_sec, (int)t2_cpu[0].tv_usec);
  printf("    lookup time (scpu):          %d.%06d\n", (int)t2_cpu[1].tv_sec, (int)t2_cpu[1].tv_usec);
  f =  t2_cpu[0].tv_sec + 1e-6 * t2_cpu[0].tv_usec;
  f += t2_cpu[1].tv_sec + 1e-6 * t2_cpu[1].tv_usec;
  printf("    throughput (lookups/cpusec): %2.2f\n", (float) lookups / f);
  rm_all_rec(c->files());

  printf("\n");
}

int main() {
  test(&sparkey_candidate_uncompressed, 1000, 1*1000*1000);
  test(&sparkey_candidate_uncompressed, 1000*1000, 1*1000*1000);
  test(&sparkey_candidate_uncompressed, 10*1000*1000, 1*1000*1000);
  test(&sparkey_candidate_uncompressed, 100*1000*1000, 1*1000*1000);

  test(&sparkey_candidate_compressed, 1000, 1*1000*1000);
  test(&sparkey_candidate_compressed, 1000*1000, 1*1000*1000);
  test(&sparkey_candidate_compressed, 10*1000*1000, 1*1000*1000);
  test(&sparkey_candidate_compressed, 100*1000*1000, 1*1000*1000);

  return 0;
}


