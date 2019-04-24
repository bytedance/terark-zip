#include <stdlib.h>
#include <cstdint>
#include "sufarr_inducedsort.h"

#if _MSC_VER
#pragma warning(disable: 4244) // shut up
#endif

#ifndef UCHAR_SIZE
# define UCHAR_SIZE 256
#endif
#ifndef MINBUCKETSIZE
# define MINBUCKETSIZE 256
#endif

#define sais_index_type int
#define sais_bool_type  int
#define SAIS_LMSSORT2_LIMIT 0x3fffffff

#define SAIS_MYMALLOC(_num, _type) ((_type *)malloc((_num) * sizeof(_type)))
#define SAIS_MYFREE(_ptr, _num, _type) free((_ptr))

namespace sufarr_inducedsort_ns {

template<class SAT>
void
getCounts(SAT *source, sais_index_type *alphabet, sais_index_type size, sais_index_type sigma) {
  sais_index_type i;
  for(i = 0; i < sigma; ++i) { alphabet[i] = 0; }
  for(i = 0; i < size; ++i) { ++alphabet[source[i]]; }
}

inline
void
getBuckets(const sais_index_type *alphabet, sais_index_type *bucket, sais_index_type sigma, sais_bool_type end) {
  sais_index_type i, sum = 0;
  if(end) { for(i = 0; i < sigma; ++i) { sum += alphabet[i]; bucket[i] = sum; } }
  else { for(i = 0; i < sigma; ++i) { sum += alphabet[i]; bucket[i] = sum - alphabet[i]; } }
}

template<class SAT>
void
LMSsort1(SAT *source, sais_index_type *suf_arr,
         sais_index_type *alphabet, sais_index_type *bucket,
         sais_index_type size, sais_index_type sigma) {
  sais_index_type *bucket_pos, i, j;
  sais_index_type alphabet_p0, alphabet_p1;

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 0);
  j = size - 1;
  bucket_pos = suf_arr + bucket[alphabet_p1 = source[j]];
  --j;
  *bucket_pos++ = (source[j] < alphabet_p1) ? ~j : j;
  for(i = 0; i < size; ++i) {
    if(0 < (j = suf_arr[i])) {
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      --j;
      *bucket_pos++ = (source[j] < alphabet_p1) ? ~j : j;
      suf_arr[i] = 0;
    } else if(j < 0) {
      suf_arr[i] = ~j;
    }
  }

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 1);
  for(i = size - 1, bucket_pos = suf_arr + bucket[alphabet_p1 = 0]; 0 <= i; --i) {
    if(0 < (j = suf_arr[i])) {
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      --j;
      *--bucket_pos = (source[j] > alphabet_p1) ? ~(j + 1) : j;
      suf_arr[i] = 0;
    }
  }
}

template<class SAT>
sais_index_type
LMSpostproc1(SAT *source, sais_index_type *suf_arr,
             sais_index_type size, sais_index_type name_size) {
  sais_index_type i, j, p, q, plen, qlen, name;
  sais_index_type alphabet_p0, alphabet_p1;
  sais_bool_type diff;

  for(i = 0; (p = suf_arr[i]) < 0; ++i) { suf_arr[i] = ~p; }
  if(i < name_size) {
    for(j = i, ++i;; ++i) {
      if((p = suf_arr[i]) < 0) {
        suf_arr[j++] = ~p; suf_arr[i] = 0;
        if(j == name_size) { break; }
      }
    }
  }

  i = size - 1; j = size - 1; alphabet_p0 = source[size - 1];
  do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
  for(; 0 <= i;) {
    do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) <= alphabet_p1));
    if(0 <= i) {
      suf_arr[name_size + ((i + 1) >> 1)] = j - i; j = i + 1;
      do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
    }
  }

  for(i = 0, name = 0, q = size, qlen = 0; i < name_size; ++i) {
    p = suf_arr[i], plen = suf_arr[name_size + (p >> 1)], diff = 1;
    if((plen == qlen) && ((q + plen) < size)) {
      for(j = 0; (j < plen) && (source[p + j] == source[q + j]); ++j) { }
      if(j == plen) { diff = 0; }
    }
    if(diff != 0) { ++name, q = p, qlen = plen; }
    suf_arr[name_size + (p >> 1)] = name;
  }

  return name;
}

template<class SAT>
void
LMSsort2(SAT *source, sais_index_type *suf_arr,
         sais_index_type *alphabet, sais_index_type *bucket, sais_index_type *bucket_prime,
         sais_index_type size, sais_index_type sigma) {
  sais_index_type *bucket_pos, i, j, t, bucket_prime_pos;
  sais_index_type alphabet_p0, alphabet_p1;

  getBuckets(alphabet, bucket, sigma, 0);
  j = size - 1;
  bucket_pos = suf_arr + bucket[alphabet_p1 = source[j]];
  --j;
  t = (source[j] < alphabet_p1);
  j += size;
  *bucket_pos++ = (t & 1) ? ~j : j;
  for(i = 0, bucket_prime_pos = 0; i < size; ++i) {
    if(0 < (j = suf_arr[i])) {
      if(size <= j) { bucket_prime_pos += 1; j -= size; }
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      --j;
      t = alphabet_p0; t = (t << 1) | (source[j] < alphabet_p1);
      if(bucket_prime[t] != bucket_prime_pos) { j += size; bucket_prime[t] = bucket_prime_pos; }
      *bucket_pos++ = (t & 1) ? ~j : j;
      suf_arr[i] = 0;
    } else if(j < 0) {
      suf_arr[i] = ~j;
    }
  }
  for(i = size - 1; 0 <= i; --i) {
    if(0 < suf_arr[i]) {
      if(suf_arr[i] < size) {
        suf_arr[i] += size;
        for(j = i - 1; suf_arr[j] < size; --j) { }
        suf_arr[j] -= size;
        i = j;
      }
    }
  }

  getBuckets(alphabet, bucket, sigma, 1);
  for(i = size - 1, bucket_prime_pos += 1, bucket_pos = suf_arr + bucket[alphabet_p1 = 0]; 0 <= i; --i) {
    if(0 < (j = suf_arr[i])) {
      if(size <= j) { bucket_prime_pos += 1; j -= size; }
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      --j;
      t = alphabet_p0; t = (t << 1) | (source[j] > alphabet_p1);
      if(bucket_prime[t] != bucket_prime_pos) { j += size; bucket_prime[t] = bucket_prime_pos; }
      *--bucket_pos = (t & 1) ? ~(j + 1) : j;
      suf_arr[i] = 0;
    }
  }
}

inline
sais_index_type
LMSpostproc2(sais_index_type *suf_arr, sais_index_type size, sais_index_type name_size) {
  sais_index_type i, j, d, name;

  for(i = 0, name = 0; (j = suf_arr[i]) < 0; ++i) {
    j = ~j;
    if(size <= j) { name += 1; }
    suf_arr[i] = j;
  }
  if(i < name_size) {
    for(d = i, ++i;; ++i) {
      if((j = suf_arr[i]) < 0) {
        j = ~j;
        if(size <= j) { name += 1; }
        suf_arr[d++] = j; suf_arr[i] = 0;
        if(d == name_size) { break; }
      }
    }
  }
  if(name < name_size) {
    for(i = name_size - 1, d = name + 1; 0 <= i; --i) {
      if(size <= (j = suf_arr[i])) { j -= size; --d; }
      suf_arr[name_size + (j >> 1)] = d;
    }
  } else {
    for(i = 0; i < name_size; ++i) {
      if(size <= (j = suf_arr[i])) { j -= size; suf_arr[i] = j; }
    }
  }

  return name;
}

template<class SAT>
void
induceSA(SAT *source, sais_index_type *suf_arr,
         sais_index_type *alphabet, sais_index_type *bucket,
         sais_index_type size, sais_index_type sigma) {
  sais_index_type *bucket_pos, i, j;
  sais_index_type alphabet_p0, alphabet_p1;

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 0);
  j = size - 1;
  bucket_pos = suf_arr + bucket[alphabet_p1 = source[j]];
  *bucket_pos++ = ((0 < j) && (source[j - 1] < alphabet_p1)) ? ~j : j;
  for(i = 0; i < size; ++i) {
    j = suf_arr[i], suf_arr[i] = ~j;
    if(0 < j) {
      --j;
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      *bucket_pos++ = ((0 < j) && (source[j - 1] < alphabet_p1)) ? ~j : j;
    }
  }

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 1);
  for(i = size - 1, bucket_pos = suf_arr + bucket[alphabet_p1 = 0]; 0 <= i; --i) {
    if(0 < (j = suf_arr[i])) {
      --j;
      if((alphabet_p0 = source[j]) != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      *--bucket_pos = ((j == 0) || (source[j - 1] > alphabet_p1)) ? ~j : j;
    } else {
      suf_arr[i] = ~j;
    }
  }
}

template<class SAT>
sais_index_type
computeBWT(SAT *source, sais_index_type *suf_arr,
           sais_index_type *alphabet, sais_index_type *bucket,
           sais_index_type size, sais_index_type sigma) {
  sais_index_type *bucket_pos, i, j, pidx = -1;
  sais_index_type alphabet_p0, alphabet_p1;

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 0);
  j = size - 1;
  bucket_pos = suf_arr + bucket[alphabet_p1 = source[j]];
  *bucket_pos++ = ((0 < j) && (source[j - 1] < alphabet_p1)) ? ~j : j;
  for(i = 0; i < size; ++i) {
    if(0 < (j = suf_arr[i])) {
      --j;
      suf_arr[i] = ~((sais_index_type)(alphabet_p0 = source[j]));
      if(alphabet_p0 != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      *bucket_pos++ = ((0 < j) && (source[j - 1] < alphabet_p1)) ? ~j : j;
    } else if(j != 0) {
      suf_arr[i] = ~j;
    }
  }

  if(alphabet == bucket) { getCounts(source, alphabet, size, sigma); }
  getBuckets(alphabet, bucket, sigma, 1);
  for(i = size - 1, bucket_pos = suf_arr + bucket[alphabet_p1 = 0]; 0 <= i; --i) {
    if(0 < (j = suf_arr[i])) {
      --j;
      suf_arr[i] = (alphabet_p0 = source[j]);
      if(alphabet_p0 != alphabet_p1) { bucket[alphabet_p1] = bucket_pos - suf_arr; bucket_pos = suf_arr + bucket[alphabet_p1 = alphabet_p0]; }
      *--bucket_pos = ((0 < j) && (source[j - 1] > alphabet_p1)) ? ~((sais_index_type)source[j - 1]) : j;
    } else if(j != 0) {
      suf_arr[i] = ~j;
    } else {
      pidx = i;
    }
  }
  return pidx;
}

template<class SAT>
sais_index_type
sais_main(SAT *source, sais_index_type *suf_arr,
          sais_index_type fs, sais_index_type size, sais_index_type sigma,
          sais_bool_type bwt) {
  sais_index_type *alphabet, *bucket, *bucket_prime, *sub_source, *bucket_pos;
  sais_index_type i, j, m, p, q, t, name, pidx = 0, newfs;
  sais_index_type alphabet_p0, alphabet_p1;
  unsigned int flags;

  if(sigma <= MINBUCKETSIZE) {
    if((alphabet = SAIS_MYMALLOC(sigma, sais_index_type)) == NULL) { return -2; }
    if(sigma <= fs) {
      bucket = suf_arr + (size + fs - sigma);
      flags = 1;
    } else {
      if((bucket = SAIS_MYMALLOC(sigma, sais_index_type)) == NULL) { SAIS_MYFREE(alphabet, sigma, sais_index_type); return -2; }
      flags = 3;
    }
  } else if(sigma <= fs) {
    alphabet = suf_arr + (size + fs - sigma);
    if(sigma <= (fs - sigma)) {
      bucket = alphabet - sigma;
      flags = 0;
    } else if(sigma <= (MINBUCKETSIZE * 4)) {
      if((bucket = SAIS_MYMALLOC(sigma, sais_index_type)) == NULL) { return -2; }
      flags = 2;
    } else {
      bucket = alphabet;
      flags = 8;
    }
  } else {
    if((alphabet = bucket = SAIS_MYMALLOC(sigma, sais_index_type)) == NULL) { return -2; }
    flags = 4 | 8;
  }
  if((size <= SAIS_LMSSORT2_LIMIT) && (2 <= (size / sigma))) {
    if(flags & 1) { flags |= ((sigma * 2) <= (fs - sigma)) ? 32 : 16; }
    else if((flags == 0) && ((sigma * 2) <= (fs - sigma * 2))) { flags |= 32; }
  }

  getCounts(source, alphabet, size, sigma); getBuckets(alphabet, bucket, sigma, 1);
  for(i = 0; i < size; ++i) { suf_arr[i] = 0; }
  bucket_pos = &t; i = size - 1; j = size; m = 0; alphabet_p0 = source[size - 1];
  do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
  for(; 0 <= i;) {
    do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) <= alphabet_p1));
    if(0 <= i) {
      *bucket_pos = j; bucket_pos = suf_arr + --bucket[alphabet_p1]; j = i; ++m;
      do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
    }
  }

  if(1 < m) {
    if(flags & (16 | 32)) {
      if(flags & 16) {
        if((bucket_prime = SAIS_MYMALLOC(sigma * 2, sais_index_type)) == NULL) {
          if(flags & (1 | 4)) { SAIS_MYFREE(alphabet, sigma, sais_index_type); }
          if(flags & 2) { SAIS_MYFREE(bucket, sigma, sais_index_type); }
          return -2;
        }
      } else {
        bucket_prime = bucket - sigma * 2;
      }
      ++bucket[source[j + 1]];
      for(i = 0, j = 0; i < sigma; ++i) {
        j += alphabet[i];
        if(bucket[i] != j) { suf_arr[bucket[i]] += size; }
        bucket_prime[i] = bucket_prime[i + sigma] = 0;
      }
      LMSsort2(source, suf_arr, alphabet, bucket, bucket_prime, size, sigma);
      name = LMSpostproc2(suf_arr, size, m);
      if(flags & 16) { SAIS_MYFREE(bucket_prime, sigma * 2, sais_index_type); }
    } else {
      LMSsort1(source, suf_arr, alphabet, bucket, size, sigma);
      name = LMSpostproc1(source, suf_arr, size, m);
    }
  } else if(m == 1) {
    *bucket_pos = j + 1;
    name = 1;
  } else {
    name = 0;
  }

  if(name < m) {
    if(flags & 4) { SAIS_MYFREE(alphabet, sigma, sais_index_type); }
    if(flags & 2) { SAIS_MYFREE(bucket, sigma, sais_index_type); }
    newfs = (size + fs) - (m * 2);
    if((flags & (1 | 4 | 8)) == 0) {
      if((sigma + name) <= newfs) { newfs -= sigma; }
      else { flags |= 8; }
    }
    sub_source = suf_arr + m + newfs;
    for(i = m + (size >> 1) - 1, j = m - 1; m <= i; --i) {
      if(suf_arr[i] != 0) {
        sub_source[j--] = suf_arr[i] - 1;
      }
    }
    if(sais_main(sub_source, suf_arr, newfs, m, name, 0) != 0) {
      if(flags & 1) { SAIS_MYFREE(alphabet, sigma, sais_index_type); }
      return -2;
    }

    i = size - 1; j = m - 1; alphabet_p0 = source[size - 1];
    do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
    for(; 0 <= i;) {
      do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) <= alphabet_p1));
      if(0 <= i) {
        sub_source[j--] = i + 1;
        do { alphabet_p1 = alphabet_p0; } while((0 <= --i) && ((alphabet_p0 = source[i]) >= alphabet_p1));
      }
    }
    for(i = 0; i < m; ++i) { suf_arr[i] = sub_source[suf_arr[i]]; }
    if(flags & 4) {
      if((alphabet = bucket = SAIS_MYMALLOC(sigma, int)) == NULL) { return -2; }
    }
    if(flags & 2) {
      if((bucket = SAIS_MYMALLOC(sigma, int)) == NULL) {
        if(flags & 1) { SAIS_MYFREE(alphabet, sigma, sais_index_type); }
        return -2;
      }
    }
  }

  if(flags & 8) { getCounts(source, alphabet, size, sigma); }
  if(1 < m) {
    getBuckets(alphabet, bucket, sigma, 1);
    i = m - 1, j = size, p = suf_arr[m - 1], alphabet_p1 = source[p];
    do {
      q = bucket[alphabet_p0 = alphabet_p1];
      while(q < j) { suf_arr[--j] = 0; }
      do {
        suf_arr[--j] = p;
        if(--i < 0) { break; }
        p = suf_arr[i];
      } while((alphabet_p1 = source[p]) == alphabet_p0);
    } while(0 <= i);
    while(0 < j) { suf_arr[--j] = 0; }
  }
  if(bwt == 0) { induceSA(source, suf_arr, alphabet, bucket, size, sigma); }
  else { pidx = computeBWT(source, suf_arr, alphabet, bucket, size, sigma); }
  if(flags & (1 | 4)) { SAIS_MYFREE(alphabet, sigma, sais_index_type); }
  if(flags & 2) { SAIS_MYFREE(bucket, sigma, sais_index_type); }

  return pidx;
}

}

/*---------------------------------------------------------------------------*/

using namespace sufarr_inducedsort_ns;

#define T_char unsigned char
#define T_uint uint32_t
#define T_iter int64_t
#define T_size int64_t
#define T_type bool
#define T_flag bool

#ifndef IMP_SIGMA
#define IMP_SIGMA 256
#endif // IMP_SIGMA

namespace gluten_sain_ns {
int neo_sais_uchar(
        T_char *S, T_uint *A,
        const T_size n, const T_size k = IMP_SIGMA);
}
namespace inner_sais_ns {
int
sufarr_inducedsort(const unsigned char *source, int *suf_arr, int size, int sigma = UCHAR_SIZE);
}
namespace terark {
extern int g_useDivSufSort;
}

extern "C" {

int
sufarr_inducedsort(const unsigned char *source, int *suf_arr, int size) {
  if (terark::g_useDivSufSort == 2)
    return inner_sais_ns::sufarr_inducedsort(source, suf_arr, size);
  else if (terark::g_useDivSufSort == 3)
    return gluten_sain_ns::neo_sais_uchar((unsigned char*)source, (uint32_t*)suf_arr, size);
  if((source == NULL) || (suf_arr == NULL) || (size < 0)) { return -1; }
  if(size <= 1) { if(size == 1) { suf_arr[0] = 0; } return 0; }
  return sais_main(source, suf_arr, 0, size, UCHAR_SIZE, 0);
}

int
sufarr_inducedsort_int(const int *source, int *suf_arr, int n, int k) {
  if((source == NULL) || (suf_arr == NULL) || (n < 0) || (k <= 0)) { return -1; }
  if(n <= 1) { if(n == 1) { suf_arr[0] = 0; } return 0; }
  return sais_main(source, suf_arr, 0, n, k, 0);
}

int
sufarr_inducedsort_bwt(const unsigned char *source, unsigned char *height, int *rank, int size) {
  int i, pidx;
  if((source == NULL) || (height == NULL) || (rank == NULL) || (size < 0)) { return -1; }
  if(size <= 1) { if(size == 1) { height[0] = source[0]; } return size; }
  pidx = sais_main(source, rank, 0, size, UCHAR_SIZE, 1);
  if(pidx < 0) { return pidx; }
  height[0] = source[size - 1];
  for(i = 0; i < pidx; ++i) { height[i + 1] = (unsigned char)rank[i]; }
  for(i += 1; i < size; ++i) { height[i] = (unsigned char)rank[i]; }
  pidx += 1;
  return pidx;
}

int
sufarr_inducedsort_int_bwt(const int *source, int *height, int *rank, int size, int sigma) {
  int i, pidx;
  if((source == NULL) || (height == NULL) || (rank == NULL) || (size < 0) || (sigma <= 0)) { return -1; }
  if(size <= 1) { if(size == 1) { height[0] = source[0]; } return size; }
  pidx = sais_main(source, rank, 0, size, sigma, 1);
  if(pidx < 0) { return pidx; }
  height[0] = source[size - 1];
  for(i = 0; i < pidx; ++i) { height[i + 1] = rank[i]; }
  for(i += 1; i < size; ++i) { height[i] = rank[i]; }
  pidx += 1;
  return pidx;
}

}

