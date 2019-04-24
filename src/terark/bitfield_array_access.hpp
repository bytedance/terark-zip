
template<int NthField>
static
typename BestUint<NthField>::type get_func(const uint8_t* base, size_t idx) {
	typedef typename BestUint<NthField>::type VAL;
	typedef uint64_t U64;
	const int MyBitPos = PrefixSum<NthField, BitFieldsBits...>::value;
	const int MyBitNum = GetNthArg<NthField, BitFieldsBits...>::value;
	const int VaBitNum = 8 * sizeof(VAL);
	const VAL MyAllOne = AllOne<MyBitNum>::value;
	// compile time const false branch will be optimized out
	if (MyBitPos % 8 + MyBitNum <= 8 && TotalBits % 8 == 0) {
		// not span byte boundary
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		if (MyBitNum == 8)
			return *p;
		else if (MyBitPos % 8 + MyBitNum == 8)
			return (*p >> MyBitPos % 8);
		else
			return (*p >> MyBitPos % 8) & MyAllOne;
	}
	else if (MyBitNum == 1 || 8 % TotalBits == 0) {
		// 8 % TotalBits == 0 : TotalBits should be in {1, 2, 4, 8}, but will not be 8
		const size_t bitPos = idx * TotalBits + MyBitPos;
		const unsigned char* p = &base[bitPos/8];
		return (*p >> bitPos%8) & MyAllOne;
	}
	else if (MyBitNum == VaBitNum && MyBitPos % VaBitNum == 0 && TotalBits % VaBitNum == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		return aligned_load<VAL>(p);
	}
	else if (MyBitNum != VaBitNum && MyBitPos % VaBitNum == 0 && TotalBits % VaBitNum == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		return aligned_load<VAL>(p) & MyAllOne;
	}
	else if (MyBitNum == VaBitNum && MyBitPos % 8 == 0 && TotalBits % 8 == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		return unaligned_load<VAL>(p);
	}
	else if (MyBitNum != VaBitNum && MyBitPos % 8 == 0 && TotalBits % 8 == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		return unaligned_load<VAL>(p) & MyAllOne;
	}
	else if (MyBitPos % VaBitNum + MyBitNum <= VaBitNum && TotalBits % VaBitNum == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		if (MyBitPos % VaBitNum + MyBitNum == VaBitNum)
			return aligned_load<VAL>(p) >> MyBitPos % VaBitNum;
		else
			return aligned_load<VAL>(p) >> MyBitPos % VaBitNum & MyAllOne;
	}
	else if (MyBitPos % VaBitNum + MyBitNum <= VaBitNum && TotalBits % 8 == 0) {
		const unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		if (MyBitPos % VaBitNum + MyBitNum == VaBitNum)
			return unaligned_load<VAL>(p) >> MyBitPos % VaBitNum;
		else
			return unaligned_load<VAL>(p) >> MyBitPos % VaBitNum & MyAllOne;
	}
	else {
		const size_t bitPos = idx * TotalBits + MyBitPos;
		const unsigned char* p = &base[bitPos/8];
		if (MyBitNum <= VaBitNum-7) {
			return VAL(unaligned_load<VAL>(p) >> bitPos%8) & MyAllOne;
		}
		else if (VaBitNum <= 32) {
			return VAL(unaligned_load<U64>(p) >> bitPos%8) & MyAllOne;
		}
		// now VAL must be uint64_t :
		else if (MyBitNum + bitPos%8 <= VaBitNum) {
			// lucky, not span uint64_t boundary
			return unaligned_load<VAL>(p) >> bitPos%8 & MyAllOne;
		}
		else { // span uint64_t boundary
			VAL v1 = unaligned_load<VAL>(p) >> bitPos%8;
			VAL v2 = VAL(p[8] & 0xFF>>(72-MyBitNum-bitPos%8)) << (64-bitPos%8);
			return v1 | v2;
		}
	}
}

template<int NthField>
static void
set_func(uint8_t* base, size_t idx, typename BestUint<NthField>::type val) {
	typedef typename BestUint<NthField>::type VAL;
	typedef uint64_t U64;
	const int MyBitPos = PrefixSum<NthField, BitFieldsBits...>::value;
	const int MyBitNum = GetNthArg<NthField, BitFieldsBits...>::value;
	const int VaBitNum = 8 * sizeof(VAL);
	const VAL MyAllOne = AllOne<MyBitNum>::value;
	assert(val <= MyAllOne);
	if (MyBitPos % 8 + MyBitNum <= 8 && TotalBits % 8 == 0) {
		// not span byte boundary
		unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		if (MyBitNum == 8)
			*p = val;
		else
			*p = (*p & ~(MyAllOne << MyBitPos % 8)) | (val << MyBitPos%8);
	}
	else if (8 % TotalBits == 0 || MyBitNum == 1) {
		// 8 % TotalBits == 0 : TotalBits should be in {1, 2, 4, 8}, but will not be 8
		const size_t bitPos = idx * TotalBits + MyBitPos;
		unsigned char* p = &base[bitPos/8];
		*p = (*p & ~(MyAllOne << bitPos % 8)) | val << (bitPos % 8);
	}
	else if (MyBitNum == VaBitNum && MyBitPos % VaBitNum == 0 && TotalBits % VaBitNum == 0) {
		unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		aligned_save(p, val);
	}
	else if (MyBitNum == VaBitNum && MyBitPos % 8 == 0 && TotalBits % 8 == 0) {
		unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		unaligned_save(p, val);
	}
	else if (MyBitPos % VaBitNum + MyBitNum <= VaBitNum && TotalBits % VaBitNum == 0) {
		unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		// aligned load and save
		VAL& r = *reinterpret_cast<VAL*>(p);
		r = (r & ~(MyAllOne << MyBitPos % VaBitNum)) | val << MyBitPos % VaBitNum;
	}
	else if (MyBitPos % VaBitNum + MyBitNum <= VaBitNum && TotalBits % 8 == 0) {
		unsigned char* p = &base[idx * (TotalBits/8) + MyBitPos/8];
		unaligned_save<VAL>(p,
			(unaligned_load<VAL>(p) & ~(MyAllOne << MyBitPos % VaBitNum))
				| val << MyBitPos % VaBitNum);
	}
	else {
		const size_t bitPos = idx * TotalBits + MyBitPos;
		unsigned char* p = &base[bitPos/8];
		if (MyBitNum <= VaBitNum-7) {
			unaligned_save<VAL>(p, (unaligned_load<VAL>(p) & ~(MyAllOne<<bitPos%8)) | val<<bitPos%8);
		}
		else if (VaBitNum <= 32) {
			unaligned_save<U64>(p, (unaligned_load<U64>(p) & ~(U64(MyAllOne)<<bitPos%8)) | U64(val)<<bitPos%8);
		}
		else { // Now VAL must be uint64 and (MyBitNum >= 58) :
			unaligned_save<VAL>(p, (unaligned_load<VAL>(p) & ~(MyAllOne<<bitPos%8)) | val<<bitPos%8);
			if (MyBitNum + bitPos%8 > VaBitNum) { // span uint64_t boundary
				p[8] = (unsigned char)
					((p[8] & 0xFF<<(MyBitNum + bitPos%8 - 64)) | val >> (64 - bitPos%8));
			}
		}
	}
}

template<class... Uints>
static
void aset_func(uint8_t* p, size_t idx, Uints... fields) {
	pset_func<0>(p, idx, fields...);
}
template<int FirstField, class FirstUint, class... OtherUints>
static
void pset_func(uint8_t* p, size_t idx, FirstUint v1, OtherUints... others) {
	set_func<FirstField>(p, idx, v1);
	pset_func<FirstField+1>(p, idx, others...);
}
template<int Field, class Uint>
static
void pset_func(uint8_t* p, size_t idx, Uint v) { set_func<Field>(p, idx, v); }

#define func_name_get0 BOOST_PP_CAT(get_func, 0)
#define func_name_set0 BOOST_PP_CAT(set_func, 0)

static typename BestUint<0>::type
func_name_get0(const uint8_t* p, size_t idx) {
	return get_func<0>(p, idx);
}
static void
func_name_set0(uint8_t* p, size_t idx, typename BestUint<0>::type val) {
	aligned_set<0>(p, idx, val);
}

#undef func_name_get0
#undef func_name_set0

