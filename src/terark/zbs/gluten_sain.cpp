#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
//#include <sys/time.h>

#ifdef _MSC_VER
# pragma warning(disable:4244 4805)
#endif

#define T_char unsigned char
#define T_uint uint32_t
#define T_iter int64_t
#define T_size int64_t
#define T_type bool
#define T_flag bool

#ifndef IMP_SIGMA
#define IMP_SIGMA 256
#endif // IMP_SIGMA

#define sais_index_type int
#define sais_bool_type  int
#define SAIS_LMSSORT2_LIMIT 0x3fffffff

#define SAIS_MYMALLOC(_num, _type) ((_type *)malloc((_num) * sizeof(_type)))
#define SAIS_MYFREE(_ptr, _num, _type) free((_ptr))
#define chr(_a) (cs == sizeof(sais_index_type) ? ((sais_index_type *)T)[(_a)] : ((unsigned char *)T)[(_a)])

namespace gluten_sain_ns
{

//struct timeval st, ed, rest, reed;

static
inline void
getCounts(const void *T, sais_index_type *C, sais_index_type n, sais_index_type k, int cs) {
  sais_index_type i;
  for(i = 0; i < k; ++i) { C[i] = 0; }
  for(i = 0; i < n; ++i) { ++C[chr(i)]; }
}

static
inline void
getBuckets(const sais_index_type *C, sais_index_type *B, sais_index_type k, sais_bool_type end) {
  sais_index_type i, sum = 0;
  if(end) { for(i = 0; i < k; ++i) { sum += C[i]; B[i] = sum; } }
  else { for(i = 0; i < k; ++i) { sum += C[i]; B[i] = sum - C[i]; } }
}

static
void
LMSsort1(const void *T, sais_index_type *SA,
         sais_index_type *C, sais_index_type *B,
         sais_index_type n, sais_index_type k, int cs) {
  sais_index_type *b, i, j;
  sais_index_type c0, c1;

  if(C == B) { getCounts(T, C, n, k, cs); }
  getBuckets(C, B, k, 0);
  j = n - 1;
  b = SA + B[c1 = chr(j)];
  --j;
  *b++ = (chr(j) < c1) ? ~j : j;
  for(i = 0; i < n; ++i) {
    if(0 < (j = SA[i])) {
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      --j;
      *b++ = (chr(j) < c1) ? ~j : j;
      SA[i] = 0;
    } else if(j < 0) {
      SA[i] = ~j;
    }
  }

  if(C == B) { getCounts(T, C, n, k, cs); }
  getBuckets(C, B, k, 1);
  for(i = n - 1, b = SA + B[c1 = 0]; 0 <= i; --i) {
    if(0 < (j = SA[i])) {
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      --j;
      *--b = (chr(j) > c1) ? ~(j + 1) : j;
      SA[i] = 0;
    }
  }
}

static
sais_index_type
LMSpostproc1(const void *T, sais_index_type *SA,
             sais_index_type n, sais_index_type m, int cs) {
  sais_index_type i, j, p, q, plen, qlen, name;
  sais_index_type c0, c1;
  sais_bool_type diff;

  for(i = 0; (p = SA[i]) < 0; ++i) { SA[i] = ~p; }
  if(i < m) {
    for(j = i, ++i;; ++i) {
      if((p = SA[i]) < 0) {
        SA[j++] = ~p; SA[i] = 0;
        if(j == m) { break; }
      }
    }
  }

  i = n - 1; j = n - 1; c0 = chr(n - 1);
  do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
  for(; 0 <= i;) {
    do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) <= c1));
    if(0 <= i) {
      SA[m + ((i + 1) >> 1)] = j - i; j = i + 1;
      do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
    }
  }

  for(i = 0, name = 0, q = n, qlen = 0; i < m; ++i) {
    p = SA[i], plen = SA[m + (p >> 1)], diff = 1;
    if((plen == qlen) && ((q + plen) < n)) {
      for(j = 0; (j < plen) && (chr(p + j) == chr(q + j)); ++j) { }
      if(j == plen) { diff = 0; }
    }
    if(diff != 0) { ++name, q = p, qlen = plen; }
    SA[m + (p >> 1)] = name;
  }

  return name;
}

static
void
LMSsort2(const void *T, sais_index_type *SA,
         sais_index_type *C, sais_index_type *B, sais_index_type *D,
         sais_index_type n, sais_index_type k, int cs) {
  sais_index_type *b, i, j, t, d;
  sais_index_type c0, c1;

  getBuckets(C, B, k, 0);
  j = n - 1;
  b = SA + B[c1 = chr(j)];
  --j;
  t = (chr(j) < c1);
  j += n;
  *b++ = (t & 1) ? ~j : j;
  for(i = 0, d = 0; i < n; ++i) {
    if(0 < (j = SA[i])) {
      if(n <= j) { d += 1; j -= n; }
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      --j;
      t = c0; t = (t << 1) | (chr(j) < c1);
      if(D[t] != d) { j += n; D[t] = d; }
      *b++ = (t & 1) ? ~j : j;
      SA[i] = 0;
    } else if(j < 0) {
      SA[i] = ~j;
    }
  }
  for(i = n - 1; 0 <= i; --i) {
    if(0 < SA[i]) {
      if(SA[i] < n) {
        SA[i] += n;
        for(j = i - 1; SA[j] < n; --j) { }
        SA[j] -= n;
        i = j;
      }
    }
  }

  getBuckets(C, B, k, 1);
  for(i = n - 1, d += 1, b = SA + B[c1 = 0]; 0 <= i; --i) {
    if(0 < (j = SA[i])) {
      if(n <= j) { d += 1; j -= n; }
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      --j;
      t = c0; t = (t << 1) | (chr(j) > c1);
      if(D[t] != d) { j += n; D[t] = d; }
      *--b = (t & 1) ? ~(j + 1) : j;
      SA[i] = 0;
    }
  }
}

static
sais_index_type
LMSpostproc2(sais_index_type *SA, sais_index_type n, sais_index_type m) {
  sais_index_type i, j, d, name;

  for(i = 0, name = 0; (j = SA[i]) < 0; ++i) {
    j = ~j;
    if(n <= j) { name += 1; }
    SA[i] = j;
  }
  if(i < m) {
    for(d = i, ++i;; ++i) {
      if((j = SA[i]) < 0) {
        j = ~j;
        if(n <= j) { name += 1; }
        SA[d++] = j; SA[i] = 0;
        if(d == m) { break; }
      }
    }
  }
  if(name < m) {
    for(i = m - 1, d = name + 1; 0 <= i; --i) {
      if(n <= (j = SA[i])) { j -= n; --d; }
      SA[m + (j >> 1)] = d;
    }
  } else {
    for(i = 0; i < m; ++i) {
      if(n <= (j = SA[i])) { j -= n; SA[i] = j; }
    }
  }

  return name;
}

static
void
induceSA(const void *T, sais_index_type *SA,
         sais_index_type *C, sais_index_type *B,
         sais_index_type n, sais_index_type k, int cs) {
  sais_index_type *b, i, j;
  sais_index_type c0, c1;

  if(C == B) { getCounts(T, C, n, k, cs); }
  getBuckets(C, B, k, 0);
  j = n - 1;
  b = SA + B[c1 = chr(j)];
  *b++ = ((0 < j) && (chr(j - 1) < c1)) ? ~j : j;
  for(i = 0; i < n; ++i) {
    j = SA[i], SA[i] = ~j;
    if(0 < j) {
      --j;
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      *b++ = ((0 < j) && (chr(j - 1) < c1)) ? ~j : j;
    }
  }

  if(C == B) { getCounts(T, C, n, k, cs); }
  getBuckets(C, B, k, 1);
  for(i = n - 1, b = SA + B[c1 = 0]; 0 <= i; --i) {
    if(0 < (j = SA[i])) {
      --j;
      if((c0 = chr(j)) != c1) { B[c1] = b - SA; b = SA + B[c1 = c0]; }
      *--b = ((j == 0) || (chr(j - 1) > c1)) ? ~j : j;
    } else {
      SA[i] = ~j;
    }
  }
}

static
sais_index_type
sais_main(const void *T, sais_index_type *SA,
          sais_index_type fs, sais_index_type n, sais_index_type k, int cs) {
  sais_index_type *C, *B, *D, *RA, *b;
  sais_index_type i, j, m, p, q, t, name, pidx = 0, newfs;
  sais_index_type c0, c1;
  unsigned int flags;

  if(k <= IMP_SIGMA) {
    if((C = SAIS_MYMALLOC(k, sais_index_type)) == NULL) { return -2; }
    if(k <= fs) {
      B = SA + (n + fs - k);
      flags = 1;
    } else {
      if((B = SAIS_MYMALLOC(k, sais_index_type)) == NULL) { SAIS_MYFREE(C, k, sais_index_type); return -2; }
      flags = 3;
    }
  } else if(k <= fs) {
    C = SA + (n + fs - k);
    if(k <= (fs - k)) {
      B = C - k;
      flags = 0;
    } else if(k <= (IMP_SIGMA * 4)) {
      if((B = SAIS_MYMALLOC(k, sais_index_type)) == NULL) { return -2; }
      flags = 2;
    } else {
      B = C;
      flags = 8;
    }
  } else {
    if((C = B = SAIS_MYMALLOC(k, sais_index_type)) == NULL) { return -2; }
    flags = 4 | 8;
  }
  if((n <= SAIS_LMSSORT2_LIMIT) && (2 <= (n / k))) {
    if(flags & 1) { flags |= ((k * 2) <= (fs - k)) ? 32 : 16; }
    else if((flags == 0) && ((k * 2) <= (fs - k * 2))) { flags |= 32; }
  }

  getCounts(T, C, n, k, cs); getBuckets(C, B, k, 1);
  for(i = 0; i < n; ++i) { SA[i] = 0; }
  b = &t; i = n - 1; j = n; m = 0; c0 = chr(n - 1);
  do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
  for(; 0 <= i;) {
    do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) <= c1));
    if(0 <= i) {
      *b = j; b = SA + --B[c1]; j = i; ++m;
      do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
    }
  }

  if(1 < m) {
    if(flags & (16 | 32)) {
      if(flags & 16) {
        if((D = SAIS_MYMALLOC(k * 2, sais_index_type)) == NULL) {
          if(flags & (1 | 4)) { SAIS_MYFREE(C, k, sais_index_type); }
          if(flags & 2) { SAIS_MYFREE(B, k, sais_index_type); }
          return -2;
        }
      } else {
        D = B - k * 2;
      }
      ++B[chr(j + 1)];
      for(i = 0, j = 0; i < k; ++i) {
        j += C[i];
        if(B[i] != j) { SA[B[i]] += n; }
        D[i] = D[i + k] = 0;
      }
      LMSsort2(T, SA, C, B, D, n, k, cs);
      name = LMSpostproc2(SA, n, m);
      if(flags & 16) { SAIS_MYFREE(D, k * 2, sais_index_type); }
    } else {
      LMSsort1(T, SA, C, B, n, k, cs);
      name = LMSpostproc1(T, SA, n, m, cs);
    }
  } else if(m == 1) {
    *b = j + 1;
    name = 1;
  } else {
    name = 0;
  }

  if(name < m) {
    if(flags & 4) { SAIS_MYFREE(C, k, sais_index_type); }
    if(flags & 2) { SAIS_MYFREE(B, k, sais_index_type); }
    newfs = (n + fs) - (m * 2);
    if((flags & (1 | 4 | 8)) == 0) {
      if((k + name) <= newfs) { newfs -= k; }
      else { flags |= 8; }
    }
    RA = SA + m + newfs;
    for(i = m + (n >> 1) - 1, j = m - 1; m <= i; --i) {
      if(SA[i] != 0) {
        RA[j--] = SA[i] - 1;
      }
    }
    if(sais_main(RA, SA, newfs, m, name, sizeof(sais_index_type)) != 0) {
      if(flags & 1) { SAIS_MYFREE(C, k, sais_index_type); }
      return -2;
    }

    i = n - 1; j = m - 1; c0 = chr(n - 1);
    do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
    for(; 0 <= i;) {
      do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) <= c1));
      if(0 <= i) {
        RA[j--] = i + 1;
        do { c1 = c0; } while((0 <= --i) && ((c0 = chr(i)) >= c1));
      }
    }
    for(i = 0; i < m; ++i) { SA[i] = RA[SA[i]]; }
    if(flags & 4) {
      if((C = B = SAIS_MYMALLOC(k, int)) == NULL) { return -2; }
    }
    if(flags & 2) {
      if((B = SAIS_MYMALLOC(k, int)) == NULL) {
        if(flags & 1) { SAIS_MYFREE(C, k, sais_index_type); }
        return -2;
      }
    }
  }

  if(flags & 8) { getCounts(T, C, n, k, cs); }
  if(1 < m) {
    getBuckets(C, B, k, 1);
    i = m - 1, j = n, p = SA[m - 1], c1 = chr(p);
    do {
      q = B[c0 = c1];
      while(q < j) { SA[--j] = 0; }
      do {
        SA[--j] = p;
        if(--i < 0) { break; }
        p = SA[i];
      } while((c1 = chr(p)) == c0);
    } while(0 <= i);
    while(0 < j) { SA[--j] = 0; }
  }
  induceSA(T, SA, C, B, n, k, cs);
  if(flags & (1 | 4)) { SAIS_MYFREE(C, k, sais_index_type); }
  if(flags & 2) { SAIS_MYFREE(B, k, sais_index_type); }

  return pidx;
}

/*---------------------------------------------------------------------------*/

int
sais(const unsigned char *T, int *SA, int n) {
  if((T == NULL) || (SA == NULL) || (n < 0)) { return -1; }
  if(n <= 1) { if(n == 1) { SA[0] = 0; } return 0; }
  return sais_main(T, SA, 0, n, IMP_SIGMA, sizeof(unsigned char));
}

int
sais_int(const int *T, int *SA, int n, int k) {
  if((T == NULL) || (SA == NULL) || (n < 0) || (k <= 0)) { return -1; }
  if(n <= 1) { if(n == 1) { SA[0] = 0; } return 0; }
  return sais_main(T, SA, 0, n, k, sizeof(int));
}

/*------------------------------------------------------------------------*/

#ifndef MAXN
#define MAXN UINT32_MAX - 1
#endif // MAXN

		inline static bool is_lms(const T_type *T, const T_iter x){return x && ~x && T[x] && !T[x-1];}

		inline static bool is_legal(const T_uint x){return x && ~x;}


	template <typename T_data>
	static inline bool equal_str(
		const T_data *S, const T_type *T,
		T_uint x, T_uint y, const T_size n)
	{
		do{if(S[x++] != S[y++]) return 0;}
		while(!is_lms(T, x) && !is_lms(T, y) && x < n && y < n);
		if(x == n || y == n) return 0;
		return S[x] == S[y];
	}

template <typename T_data>
static int neo_sais_core(
	T_data *S, T_uint *A,
	const T_size n, const T_size k)
{
	T_uint *B, *C;
	T_iter i, j, lx = -1;
	T_size m = 0, nm;

	T_uint *P; T_type *T;
	if(NULL == (P =(T_uint *)malloc(sizeof(T_uint) * n))) return -5;
	if(NULL == (T =(T_type *)malloc(sizeof(T_type) * n))) return -6;

	T[n-1] = 0;
	for(i = n-2; i >= 0; i--) T[i] = S[i] < S[i+1] || (S[i] == S[i+1] && T[i+1]);
	for(i = 1; i < n; i++) if(T[i] && !T[i-1]) P[m++] = i;

	B = P + m;
	memset(B, 0, sizeof(T_uint) * k);
	for(i = 0; i < n; i++) B[S[i]]++;
	for(i = 1; i < k; i++) B[i] += B[i-1];

	C = B + k;
	memcpy(C, B, sizeof(T_uint) * k);

	memset(A, T_uint(-1), sizeof(T_uint) * n);
	for(i = 0; i < m; i++) A[--C[S[P[i]]]] = P[i];

	C[0] = 0; memcpy(C + 1, B, sizeof(T_uint) * (k - 1));
	A[C[S[n-1]]++] = n-1;
	for(i = 0; i < n; i++) if(is_legal(A[i]) && !T[A[i]-1]) A[C[S[A[i]-1]]++] = A[i]-1;
	memcpy(C, B, sizeof(T_uint) * k);
	for(i = n-1; i >= 0; i--) if(is_legal(A[i]) && T[A[i]-1]) A[--C[S[A[i]-1]]] = A[i]-1;
	bool flag = 1; for(i = 0; i < n; i++) if(~A[i]) {flag = 0; break;}
	if(m | flag){
  int* RS = (int *)malloc(sizeof(int) * n >> 1);
  int* RA = (int *)malloc(sizeof(int) * m);
  j = nm = 0;
	memset(RS, T_uint(-1), sizeof(T_uint) * n >> 1);
	while(!is_lms(T, A[j])) j++;
	lx = A[j]; RS[A[j] >> 1] = nm;
	for(i = j + 1; i < n; i++)
	if(is_lms(T, A[i])){
		if(!equal_str(S, T, A[i], lx, n)) nm++;
		lx = A[i]; RS[A[i] >> 1] = nm;
	}nm++;
	for(i = j = 0; i <= n >> 1; i++) if(~RS[i]) RS[j++] = RS[i];
  //gettimeofday(&rest, NULL);
	if(nm < m) sais_int(RS, RA, m, nm);
	else for(i = 0; i < m; i++) RA[RS[i]] = i;
  //gettimeofday(&reed, NULL);
	memset(A, T_uint(-1), sizeof(T_uint) * n);  
	memcpy(C, B, sizeof(T_uint) * k);
	for(i = m-1; i >= 0; i--) A[--C[S[P[RA[i]]]]] = P[RA[i]];

	C[0] = 0; memcpy(C + 1, B, sizeof(T_uint) * (k - 1));
	A[C[S[n-1]]++] = n-1;
	for(i = 0; i < n; i++) if(is_legal(A[i]) && !T[A[i]-1]) A[C[S[A[i]-1]]++] = A[i]-1;

	memcpy(C, B, sizeof(T_uint) * k);
	for(i = n-1; i >= 0; i--) if(is_legal(A[i]) && T[A[i]-1]) A[--C[S[A[i]-1]]] = A[i]-1;

  free(P); free(T); free(RS); free(RA);
	}
	return 0;
}

int neo_sais_uchar(
	T_char *S, T_uint *A,
	const T_size n, const T_size k = IMP_SIGMA)
{
	if(S == NULL) return -1;
	if(A == NULL) return -2;
	if(n < 1 || n > MAXN) return -3;
	if(k < 1 || k > IMP_SIGMA) return -4;

	if(n == 1) {A[0] = 0; return 0;}
  if(n >= INT32_MAX)
	  neo_sais_core(S, A, n, k);
  else
    sais(S, reinterpret_cast<int*>(A), n);
	return 0;
}

}

