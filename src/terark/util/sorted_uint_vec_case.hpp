{
#if !defined(NDEBUG)
	size_t dbgWidth     = Width;       TERARK_UNUSED_VAR(dbgWidth);
	size_t dbgWordUnits = WordUnits;   TERARK_UNUSED_VAR(dbgWordUnits);
#endif
	size_t headerLen;
	size_t const minDiffVal = GetLoWater_x(header, &headerLen) - 1;
	size_t const* pData = (const size_t*)(uintptr_t(header) + headerLen);
	size_t const subWords = subIdx / WordUnits;
#if TERARK_WORD_BITS % Width != 0
	size_t const baseWords = (blockUnits / 64) * Width; // trick
  #if Width >= 10
	size_t const scanBaseWords = std::min(subWords, baseWords);
	size_t extraBitsA[4] = {0};
  #else
	// extraBits1 is last words, and is not full-filled
	size_t const scanBaseWords = subWords;
	size_t extraBits1 = 0;
  #endif
	size_t largeBitPos = (headerLen + offset0) * 8 + baseWords * 64;
	assert(largeBitPos <= 8 * offset1);
#else
	assert(blockUnits * Width <= 8 * (offset1 - offset0 - headerLen));
	size_t largeBitPos = (headerLen + offset0) * 8 + blockUnits * Width - Width;
	size_t const scanBaseWords = subWords;
#endif

	size_t largeRank = 0;
	size_t ddsum = 0;
	size_t i;
	for(i = 0; i < scanBaseWords; ++i) {
		size_t w = unaligned_load<size_t>(&pData[i]);
	#if TERARK_WORD_BITS % Width != 0
	  #if Width >= 10
		static_assert((64 - 64%Width) % (64%Width) == 0, "Good luck, this should always be true");
		static const size_t NumExtrasInOneWord = (64 - 64%Width) / (64%Width);
		extraBitsA[i / NumExtrasInOneWord]
				   |= (w >> (64 - 64%Width)) << (64%Width) * (i % NumExtrasInOneWord);
	  #else
		extraBits1 |= (w >> (64 - 64%Width)) << (64%Width) * i;
	  #endif
	#endif
    #define RealUnitCount WordUnits
		#include "sorted_uint_vec_word.hpp"
    #undef RealUnitCount
	}
  #if TERARK_WORD_BITS % Width != 0
		assert(i <= subWords);
	#if Width >= 10
		for (; i < subWords; ++i) {
			size_t w = extraBitsA[i - baseWords];
          #define RealUnitCount WordUnits
			#include "sorted_uint_vec_word.hpp" // will use i
          #undef RealUnitCount
		}
		size_t w = i < baseWords ? unaligned_load<size_t>(&pData[i]) : extraBitsA[i-baseWords];
	#else
		size_t w = i < baseWords ? unaligned_load<size_t>(&pData[i]) : extraBits1;
	#endif
  #else
		size_t w = unaligned_load<size_t>(&pData[i]);
  #endif

	// count of real units is (blockUnits-1)
	// unit.nth(blockUnits-1) is not stored
	if (subIdx + 1 < blockUnits) {
		size_t ddcur;
#if TERARK_WORD_BITS % Width != 0
		// faster than subIdx % Width if Width is not power of 2
		const size_t RealUnitCount = subIdx - WordUnits * i;
		assert(RealUnitCount == subIdx % WordUnits);
#else
		// faster if Width is power of 2
		const size_t RealUnitCount = subIdx % WordUnits;
#endif
		if (RealUnitCount) {
			ddcur = UnitMask & (w >> (Width*RealUnitCount));
			// highest unit must be zero
			#include "sorted_uint_vec_word.hpp"
		}
		else {
			ddcur = UnitMask & w;
		}
		size_t diff;
		if (terark_likely(0 != ddcur && 0 == largeRank)) {
			aVal[0] = sample0 + ddsum + minDiffVal * subIdx;
			diff = minDiffVal + ddcur;
		}
		else {
			size_t const largeUnitWidth = Width +
				febitvec::s_get_uint(pLargeBase, largeBitPos, 6);
			largeBitPos += 6;
			for (i = 0; i < largeRank; ++i) {
				ddsum += febitvec::s_get_uint(pLargeBase, largeBitPos, largeUnitWidth);
				largeBitPos += largeUnitWidth;
			}
			aVal[0] = sample0 + ddsum + minDiffVal * (subIdx - largeRank);
			if (ddcur) {
				diff = minDiffVal + ddcur;
			} else {
				diff = febitvec::s_get_uint(pLargeBase, largeBitPos, largeUnitWidth);
			}
		}
		aVal[1] = aVal[0] + diff;
	}
	else {
#if TERARK_WORD_BITS % Width != 0
		// faster than subIdx % Width if Width is not power of 2
		const size_t RealUnitCount = subIdx - WordUnits * i;
		assert(RealUnitCount == subIdx % WordUnits);
		if (RealUnitCount) {
			// highest unit must be zero
			#include "sorted_uint_vec_word.hpp"
		}
#else
		const size_t RealUnitCount = WordUnits-1;
		#include "sorted_uint_vec_word.hpp"
#endif
		if (terark_likely(0 == largeRank)) {
			aVal[0] = sample0 + ddsum + minDiffVal * subIdx;
		}
		else {
			size_t const largeUnitWidth = Width +
				febitvec::s_get_uint(pLargeBase, largeBitPos, 6);
			largeBitPos += 6;
			for (i = 0; i < largeRank; ++i) {
				ddsum += febitvec::s_get_uint(pLargeBase, largeBitPos, largeUnitWidth);
				largeBitPos += largeUnitWidth;
			}
			aVal[0] = sample0 + ddsum + minDiffVal * (subIdx - largeRank);
		}
		aVal[1] = sample1;
		assert(aVal[0] <= aVal[1] || !m_is_overall_full_sorted);
	}
	break;
}

#undef Width
