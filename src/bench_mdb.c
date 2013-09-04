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

#ifdef __APPLE__
#include <mach/mach_time.h>
static float wall() {
  static double multiplier = 0;
  if (multiplier <= 0) {
    mach_timebase_info_data_t info;
    mach_timebase_info(&info);
    multiplier = (double) info.numer / (double) info.denom / 1000000000.0;
  }
  return (float) (multiplier * mach_absolute_time());
}
static float cpu() {
  return wall();
}

#else

#include <time.h>
#ifdef CLOCK_MONOTONIC_RAW
#define CLOCK_SUITABLE CLOCK_MONOTONIC_RAW
#else
#define CLOCK_SUITABLE CLOCK_MONOTONIC
#endif
static float wall() {
  struct timespec tp;
  clock_gettime(CLOCK_SUITABLE, &tp);
  return tp.tv_sec + 1e-9 * tp.tv_nsec;
}

static float cpu() {
  struct timespec tp;
  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tp);
  return tp.tv_sec + 1e-9 * tp.tv_nsec;
}
#endif

typedef struct {
  char *name;
  void (*create)(int n);
  void (*randomaccess)(int n, int lookups);
  const char** (*files)();
} candidate;

/* LMDB stuff */

#include <lmdb.h>

static void _mdb_assert(const char *file, int line, int i) {
  if (i != MDB_SUCCESS) {
    printf("%s:%d: assertion failed: %s\n", file, line, mdb_strerror(i));
    exit(i);
  }
}

#define mdb_assert(i) _mdb_assert(__FILE__, __LINE__, i)

static size_t dsize;

static void mdb_create(int n) {
  MDB_env *env;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_cursor *mc;
  MDB_val key, val;
  MDB_stat ms;
  MDB_envinfo info;
  char mykey[100];
  char myvalue[100];
  int i, j;

  key.mv_data = mykey;
  val.mv_data = myvalue;
  mdb_assert(mdb_env_create(&env));
  mdb_assert(mdb_env_set_mapsize(env, n * 64L * 2));	/* fudge */
  mdb_assert(mdb_env_open(env, "test.mdb", MDB_NOSYNC|MDB_WRITEMAP|MDB_NOSUBDIR, 0664));
  mdb_assert(mdb_txn_begin(env, NULL, 0, &txn));
  mdb_assert(mdb_dbi_open(txn, NULL, 0, &dbi));
  mdb_assert(mdb_cursor_open(txn, dbi, &mc));
  for (i = 0, j=0; i < n; i++,j++) {
    key.mv_size = sprintf(mykey, "key_%09d", i);
    val.mv_size = sprintf(myvalue, "value_%d", i);
    mdb_assert(mdb_cursor_put(mc, &key, &val, MDB_APPEND));
	if (j==999) {
	  j = 0;
	  mdb_assert(mdb_txn_commit(txn));
      mdb_assert(mdb_txn_begin(env, NULL, 0, &txn));
      mdb_assert(mdb_cursor_open(txn, dbi, &mc));
	}
  }
  mdb_txn_commit(txn);
  mdb_env_stat(env, &ms);
  mdb_env_info(env, &info);
  dsize = ms.ms_psize * info.me_last_pgno;
  mdb_env_close(env);
}

static void mdb_randomaccess(int n, int lookups) {
  MDB_env *env;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_cursor *mc;
  char mykey[100];
  char myvalue[100];
  MDB_val key, val;
  int i;

  mdb_assert(mdb_env_create(&env));
  mdb_assert(mdb_env_open(env, "test.mdb", MDB_RDONLY|MDB_NOSUBDIR, 0664));
  mdb_assert(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
  mdb_assert(mdb_dbi_open(txn, NULL, 0, &dbi));
  mdb_assert(mdb_cursor_open(txn, dbi, &mc));

  key.mv_data = mykey;

  for (i = 0; i < lookups; i++) {
    int r = rand() % n;
	int vlen;
	key.mv_size = sprintf(mykey, "key_%09d", r);
    vlen = sprintf(myvalue, "value_%d", r);
    mdb_assert(mdb_cursor_get(mc, &key, &val, MDB_SET));

    if (val.mv_size != vlen || memcmp(myvalue, val.mv_data, val.mv_size)) {
      printf("Did not get the expected value for key: %s\n", mykey);
      exit(1);
    }
  }
  mdb_cursor_close(mc);
  mdb_txn_abort(txn);
  mdb_env_close(env);
}

static void mdb_create_uncompressed(int n) {
  mdb_create(n);
}

#if 0
static void mdb_create_compressed(int n) {
  mdb_create(n, SPARKEY_COMPRESSION_SNAPPY, 1024);
}
#endif

static const char* mdb_list[] = {"test.mdb", NULL};

static const char** mdb_files() {
  return mdb_list;
}

static candidate mdb_candidate_uncompressed = {
  "LMDB uncompressed", &mdb_create_uncompressed, &mdb_randomaccess, &mdb_files
};

#if 0
static candidate mdb_candidate_compressed = {
  "Sparkey compressed(1024)", &mdb_create_compressed, &mdb_randomaccess, &mdb_files
};
#endif

/* main */

void test(candidate *c, int n, int lookups) {
  printf("Testing bulk insert of %d elements and %d random lookups\n", n, lookups);

  printf("  Candidate: %s\n", c->name);
  rm_all_rec(c->files());

  float t1_wall = wall();
  float t1_cpu = cpu();

  c->create(n);

  float t2_wall = wall();
  float t2_cpu = cpu();
  printf("    creation time (wall):     %2.2f\n", t2_wall - t1_wall);
  printf("    creation time (cpu):      %2.2f\n", t2_cpu - t1_cpu);
  printf("    throughput (puts/cpusec): %2.2f\n", (float) n / (t2_cpu - t1_cpu));
  printf("    data size:                %zu\n", dsize);

  c->randomaccess(n, lookups);

  float t3_wall = wall();
  float t3_cpu = cpu();
  printf("    lookup time (wall):          %2.2f\n", t3_wall - t2_wall);
  printf("    lookup time (cpu):           %2.2f\n", t3_cpu - t2_cpu);
  printf("    throughput (lookups/cpusec): %2.2f\n", (float) lookups / (t3_cpu - t2_cpu));
  rm_all_rec(c->files());

  printf("\n");
}

int main() {
  test(&mdb_candidate_uncompressed, 1000, 1*1000*1000);
  test(&mdb_candidate_uncompressed, 1000*1000, 1*1000*1000);
  test(&mdb_candidate_uncompressed, 10*1000*1000, 1*1000*1000);
  test(&mdb_candidate_uncompressed, 100*1000*1000, 1*1000*1000);

#if 0
  test(&mdb_candidate_compressed, 1000, 1*1000*1000);
  test(&mdb_candidate_compressed, 1000*1000, 1*1000*1000);
  test(&mdb_candidate_compressed, 10*1000*1000, 1*1000*1000);
  test(&mdb_candidate_compressed, 100*1000*1000, 1*1000*1000);
#endif

  return 0;
}


