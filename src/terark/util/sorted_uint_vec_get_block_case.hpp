{
#if !defined(NDEBUG)
    size_t dbgWidth = Width;       TERARK_UNUSED_VAR(dbgWidth);
    size_t dbgWordUnits = WordUnits;   TERARK_UNUSED_VAR(dbgWordUnits);
#endif
    size_t headerLen;
    size_t const baseWords = (blockUnits / 64) * Width; // trick
#if Width == 1
    size_t const smallDiff = GetLoWater_x(header, &headerLen);
#else
    size_t const minDiffVal = GetLoWater_x(header, &headerLen) - 1;
#endif
    size_t const* pData = (const size_t*)(uintptr_t(header) + headerLen);
#if TERARK_WORD_BITS % Width != 0
  #if Width >= 10
    size_t extraBitsA[4] = { 0 };
  #else
    // extraBits1 is last words, and is not full-filled
    size_t extraBits1 = 0;
  #endif
    size_t largeBitPos = (headerLen + offset0) * 8 + baseWords * 64;
    assert(largeBitPos <= 8 * offset1);
#else
    assert(blockUnits * Width <= 8 * (offset1 - offset0 - headerLen));
    size_t largeBitPos = (headerLen + offset0) * 8 + blockUnits * Width - Width;
#endif
    size_t val = sample0;
    size_t largeUnitWidth = Width + febitvec::s_get_uint(pLargeBase, largeBitPos, 6);
    largeBitPos += 6;

    size_t i = 0;
    for(; i < baseWords; ++i) {
        size_t w = unaligned_load<size_t>(&pData[i]);
        size_t const RealWordUnits = WordUnits;
        #include "sorted_uint_vec_get_block_word.hpp"
    #if TERARK_WORD_BITS % Width != 0
      #if Width >= 10
        static_assert((64 - 64 % Width) % (64 % Width) == 0, "Good luck, this should always be true");
        static const size_t NumExtrasInOneWord = (64 - 64 % Width) / (64 % Width);
        extraBitsA[i / NumExtrasInOneWord] |= w << (64 % Width) * (i % NumExtrasInOneWord);
      #else
        extraBits1 |= w << (64 % Width) * i;
      #endif
    #endif
    }
#if TERARK_WORD_BITS % Width != 0
    #if Width >= 10
        size_t XextWords = blockUnits / WordUnits;
        for(; i < XextWords; ++i) {
            size_t w = extraBitsA[i - baseWords];
            size_t const RealWordUnits = WordUnits;
            #include "sorted_uint_vec_get_block_word.hpp" // will use i
        }
        size_t w = extraBitsA[i - baseWords];
        size_t RealWordUnits = blockUnits % WordUnits;
    #else
        size_t baseUnits = WordUnits * baseWords;
        size_t RealWordUnits = blockUnits - baseUnits;
        assert(RealWordUnits < WordUnits);
        size_t w = extraBits1;
    #endif
        #include "sorted_uint_vec_get_block_word.hpp" // will use i
#endif
    break;
}

#undef Width
