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
  char myvalue[100];
  int i, j;

  key.mv_data = &i;
  key.mv_size = sizeof(i);
  val.mv_data = myvalue;
  mdb_assert(mdb_env_create(&env));
  mdb_assert(mdb_env_set_mapsize(env, n * 64L * 2));	/* fudge */
  mdb_assert(mdb_env_open(env, "test.mdb", MDB_NOSYNC|MDB_WRITEMAP|MDB_NOSUBDIR, 0664));
  mdb_assert(mdb_txn_begin(env, NULL, 0, &txn));
  mdb_assert(mdb_dbi_open(txn, NULL, MDB_INTEGERKEY, &dbi));
  mdb_assert(mdb_cursor_open(txn, dbi, &mc));
  for (i = 0, j=0; i < n; i++,j++) {
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
  int i, r;

  mdb_assert(mdb_env_create(&env));
  mdb_assert(mdb_env_open(env, "test.mdb", MDB_RDONLY|MDB_NOSUBDIR, 0664));
  mdb_assert(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
  mdb_assert(mdb_dbi_open(txn, NULL, 0, &dbi));
  mdb_assert(mdb_cursor_open(txn, dbi, &mc));

  key.mv_data = &r;
  key.mv_size = sizeof(r);

  for (i = 0; i < lookups; i++) {
    r = rand() % n;
	int vlen;
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
  printf("    data size:                %zu\n", dsize);

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


