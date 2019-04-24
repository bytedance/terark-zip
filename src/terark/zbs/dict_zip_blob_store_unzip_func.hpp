template<int gOffsetBytes>
terark_no_inline
terark_flatten static void
DoUnzipFuncName(const byte_t* pos, const byte_t* end, valvec<byte_t>* recData,
                const byte_t* dic,
                size_t gOffsetBits, size_t reserveOutputMultiplier)
{
	DzType_Trace("DeCompress %zd\n", size_t(end - pos));
    assert(pos <= end);

    if (terark_unlikely(pos == end)) {
        return;
    }
	_mm_prefetch((char const*)pos, _MM_HINT_T0);
	auto gLenBitsInOffset = gOffsetBytes*8 - gOffsetBits;
	auto gLenMaskInOffset = (size_t(1) << gLenBitsInOffset) - 1;
	auto gShortLenBits = 5 + gLenBitsInOffset;
	auto gShortLenMask = (size_t(1) << gShortLenBits) - 1;

#if UnzipUseThreading
  #if 0
    // threading using relative is slower
    static const int jumpOffsets[8] = {
        int((char*)&&JumpLiteral    - (char*)&&JumpBase), // 0
        int((char*)&&JumpGlobal     - (char*)&&JumpBase), // 1
        int((char*)&&JumpRLE        - (char*)&&JumpBase), // 2
        int((char*)&&JumpNearShort  - (char*)&&JumpBase), // 3
        int((char*)&&JumpFar1Short  - (char*)&&JumpBase), // 4
        int((char*)&&JumpFar2Short  - (char*)&&JumpBase), // 5
        int((char*)&&JumpFar2Long   - (char*)&&JumpBase), // 6
        int((char*)&&JumpFar3Long   - (char*)&&JumpBase), // 7
    };
    JumpBase:
    #define JumpTarget (void*)((char*)&&JumpBase + jumpOffsets[b&7])
  #else
    static const void* jumpTargets[8] = {
        &&JumpLiteral    , // 0
        &&JumpGlobal     , // 1
        &&JumpRLE        , // 2
        &&JumpNearShort  , // 3
        &&JumpFar1Short  , // 4
        &&JumpFar2Short  , // 5
        &&JumpFar2Long   , // 6
        &&JumpFar3Long   , // 7
    };
    #define JumpTarget jumpTargets[b&7]
  #endif

  #define JumpToNext() \
    Inc_output(); \
    if (terark_likely(pos < end)) { \
        b = *pos++; \
        goto *JumpTarget; \
    } else { \
        goto Done; \
    }
  #define JumpLabel(name) Jump##name

#else // do not use threading

  #define JumpToNext() Inc_output(); break
 #if UnzipDelayGlobalMatch
  #define JumpLabel(name) case (int)DzType::name
 #else
  #define JumpLabel(name) case DzType::name
 #endif
#endif // UnzipUseThreading

#if UnzipReserveBuffer || !defined(NDEBUG)
	const size_t oldsize = recData->size();
#endif
#if UnzipReserveBuffer
    recData->ensure_capacity(oldsize + (end-pos)*reserveOutputMultiplier);
    auto output = recData->data() + oldsize;
    auto outEnd = recData->data() + recData->capacity();
    #define Inc_output() output += len
    #define CheckOutputCapacity() \
        if (terark_unlikely(output + len > outEnd)) \
            outEnd = UpdateOutputPtrAfterGrowCapacity(recData, len, output)
    #define DbgRecDataSize size_t(output - recData->data())
#else
    #define Inc_output()
    #define CheckOutputCapacity() auto output = recData->grow_no_init(len)
    #define DbgRecDataSize recData->size()
#endif

// random RAM access speed is very slower than CPU.
// global dict hit is random, we prefetch global dict, and continue
// execute unzipping...
#if UnzipDelayGlobalMatch
    #if UnzipUseThreading != 0
        #error "UnzipUseThreading must not 0"
    #endif
    size_t last_gDicOffset = size_t(-1);
    size_t last_gLength = 0;
    #define DzTypeValue ((0!=last_gLength)<<3) | (b&7)
#else
    #define DzTypeValue DzType(b&7)
#endif

#if UnzipUseThreading
    size_t b = *pos++;
    goto *JumpTarget;
#else
    do {
        size_t b = *pos++;
        switch (DzTypeValue) {
#endif

JumpLabel(Literal):
    {
        size_t  len = (b >> 3) + 1;
        DzType_Trace("%zd Literal %zd\n", DbgRecDataSize, len);
        CheckOutputCapacity();
        small_memcpy(output, pos, len);
        pos += len;
        JumpToNext();
    }
JumpLabel(Global):
    {
        size_t offset = unaligned_load<uint32_t>(pos);
        size_t len = ((offset & gLenMaskInOffset) << 5) | (b >> 3);
        if (gOffsetBytes == 3) {
			offset = (offset & 0x00FFFFFF) >> gLenBitsInOffset;
        }
        else {
			offset >>= gLenBitsInOffset;
        }
        assert(offset < tg_dicLen);
        _mm_prefetch((char const*)dic + offset, _MM_HINT_T0);
        pos += gOffsetBytes;
        if (terark_likely(len < gShortLenMask)) {
            len += gMinLen;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += gMinLen + gShortLenMask;
        }
        DzType_Trace("%zd Global %zd %zd\n", DbgRecDataSize, offset, len);
        CheckOutputCapacity();
    #if UnzipDelayGlobalMatch
		(void)output; // unused when AutoGrow
        last_gLength = len;
        last_gDicOffset = offset;
    #else
        small_memcpy(output, dic + offset, len);
    #endif
        JumpToNext();
    }
JumpLabel(RLE):
    {
        size_t len = (b >> 3) + 2;
        DzType_Trace("%zd RLE %zd\n", DbgRecDataSize, len);
        CheckOutputCapacity();
        memset(output, output[-1], len);
        JumpToNext();
    }
JumpLabel(NearShort):
    {
        size_t len = ((b >> 3) & 3) + 2;
        size_t distance = (b >> 5) + 2;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd NearShort %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
JumpLabel(Far1Short):
    {
        size_t len = (b >> 3) + 2;
        size_t distance = 2 + *pos++;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far1Short %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
JumpLabel(Far2Short):
    {
        size_t len = (b >> 3) + 2;
        size_t distance = 258 + unaligned_load<uint16_t>(pos);
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far2Short %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 2;
        small_memcpy(output, output - distance, len); // distance >= 258
        JumpToNext();
    }
JumpLabel(Far2Long):
    {
        size_t len = b >> 3;
        if (terark_likely(len < 31)) {
            len += 34;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += 65;
        }
        size_t distance = unaligned_load<uint16_t>(pos);
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far2Long %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 2;
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
JumpLabel(Far3Long):
    {
        size_t len = b >> 3;
        if (terark_likely(len < 31)) {
            len += 5;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += 36;
        }
        size_t distance = unaligned_load<uint32_t>(pos) & 0xFFFFFF;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far3Long %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 3;
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
#if UnzipDelayGlobalMatch
case 8 + (int)DzType::Literal:
    {
        assert(0 != last_gLength);
        size_t  len = (b >> 3) + 1;
        DzType_Trace("%zd Literal %zd\n", DbgRecDataSize, len);
        CheckOutputCapacity();
        small_memcpy(output, pos, len);
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = 0;
        pos += len;
        JumpToNext();
    }
case 8 + (int)DzType::Global:
    {
        size_t offset = unaligned_load<uint32_t>(pos);
        size_t len = ((offset & gLenMaskInOffset) << 5) | (b >> 3);
        if (gOffsetBytes == 3) {
            offset = (offset & 0x00FFFFFF) >> gLenBitsInOffset;
        }
        else {
            offset >>= gLenBitsInOffset;
        }
        assert(offset < tg_dicLen);
        _mm_prefetch((char const*)dic + offset, _MM_HINT_T0);
        pos += gOffsetBytes;
        if (terark_likely(len < gShortLenMask)) {
            len += gMinLen;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += gMinLen + gShortLenMask;
        }
        DzType_Trace("%zd Global %zd %zd\n", DbgRecDataSize, offset, len);
        CheckOutputCapacity();
        //small_memcpy(output, dic + offset, len);
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = len;
        last_gDicOffset = offset;
        JumpToNext();
    }
case 8 + (int)DzType::RLE:
    {
        size_t len = (b >> 3) + 2;
        DzType_Trace("%zd RLE %zd\n", DbgRecDataSize, len);
        CheckOutputCapacity();
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = 0;
        memset(output, output[-1], len);
        JumpToNext();
    }
case 8 + (int)DzType::NearShort:
    {
        size_t len = ((b >> 3) & 3) + 2;
        size_t distance = (b >> 5) + 2;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd NearShort %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = 0;
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
case 8 + (int)DzType::Far1Short:
    {
        size_t len = (b >> 3) + 2;
        size_t distance = 2 + *pos++;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far1Short %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = 0;
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
case 8 + (int)DzType::Far2Short:
    {
        size_t len = (b >> 3) + 2;
        size_t distance = 258 + unaligned_load<uint16_t>(pos);
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far2Short %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 2;
        if (terark_likely(distance >= last_gLength + len)) {
            // has no dependency, copy local first
            small_memcpy(output, output - distance, len); // distance >= 258
            small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        } else {
            small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
            small_memcpy(output, output - distance, len); // distance >= 258
        }
        last_gLength = 0;
        JumpToNext();
    }
case 8 + (int)DzType::Far2Long:
    {
        size_t len = b >> 3;
        if (terark_likely(len < 31)) {
            len += 34;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += 65;
        }
        size_t distance = unaligned_load<uint16_t>(pos);
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far2Long %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 2;
        if (terark_likely(distance >= last_gLength + len)) {
            // has no dependency, copy local first
            small_memcpy(output, output - distance, len);
            small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        } else {
            small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
            CopyForward(output - distance, output, len);
        }
        last_gLength = 0;
        JumpToNext();
    }
case 8 + (int)DzType::Far3Long:
    {
        size_t len = b >> 3;
        if (terark_likely(len < 31)) {
            len += 5;
        }
        else {
            FAST_READ_VAR_UINT32(pos, len);
            len += 36;
        }
        size_t distance = unaligned_load<uint32_t>(pos) & 0xFFFFFF;
        assert(distance <= DbgRecDataSize - oldsize);
        DzType_Trace("%zd Far3Long %zd %zd\n", DbgRecDataSize, distance, len);
        CheckOutputCapacity();
        pos += 3;
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
        last_gLength = 0;
        CopyForward(output - distance, output, len);
        JumpToNext();
    }
#endif

#if UnzipUseThreading
Done:
#else
}} while (pos < end);
#endif
assert(pos == end);

#if UnzipDelayGlobalMatch
    if (last_gLength) {
  #if !UnzipReserveBuffer
        auto output = recData->end();
  #endif
        small_memcpy(output - last_gLength, dic + last_gDicOffset, last_gLength);
    }
#endif

#if UnzipReserveBuffer
    recData->risk_set_size(output - recData->data());
#endif
}

#undef DbgRecDataSize
#undef Inc_output
#undef CheckOutputCapacity
#undef JumpLabel
#undef JumpTarget
#undef JumpToNext
#undef DzTypeValue

#undef UnzipReserveBuffer
#undef UnzipUseThreading
#undef UnzipDelayGlobalMatch
#undef DoUnzipFuncName

