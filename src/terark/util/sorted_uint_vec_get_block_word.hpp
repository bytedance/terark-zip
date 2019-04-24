#if (Width == 1)
for (size_t j = 0; j < RealWordUnits; ++j) {
    aVals[i*TERARK_WORD_BITS + j] = val;
    if (w & 1) {
        val += smallDiff; // faster than Width != 1
    }
    else {
        size_t largeDiff = febitvec::s_get_uint(pLargeBase, largeBitPos, largeUnitWidth);
        val += largeDiff;
        largeBitPos += largeUnitWidth;
    }
    w >>= 1;
}
#else
for (size_t j = 0; j < RealWordUnits; ++j) {
    aVals[i*WordUnits + j] = val;
    size_t diff = w & UnitMask;
    if (diff) {
        val += minDiffVal + diff;
    }
    else {
        size_t largeDiff = febitvec::s_get_uint(pLargeBase, largeBitPos, largeUnitWidth);
        val += largeDiff;
        largeBitPos += largeUnitWidth;
    }
    w >>= Width;
}
#endif
