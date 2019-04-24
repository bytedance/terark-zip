  #if !defined(NDEBUG)
        size_t dbgRealUnitCount = RealUnitCount; TERARK_UNUSED_VAR(dbgRealUnitCount);
  #endif
  #if !defined(RealUnitCount)
        assert(RealUnitCount < WordUnits);
        w &= ~(size_t(-1) << (Width*RealUnitCount));
  #endif
  #if defined(RealUnitCount) && WordUnits % 2 != 0
    #define PlusHighestUnit() + ((w >> (64 - 64%Width - Width)) & UnitMask)
    #if !defined(NDEBUG)
        size_t dbgHighestUnit = PlusHighestUnit(); TERARK_UNUSED_VAR(dbgHighestUnit);
    #endif
  #else
    #define PlusHighestUnit()
  #endif
		size_t z; // zero unit count
	#if (Width == 1)
		w = fast_popcount64(w);
		z = RealUnitCount - w;
	#elif (Width == 2)
		z = RealUnitCount - fast_popcount64(
			(w & 0x5555555555555555ull) | ((w >> 1) & 0x5555555555555555ull));
		w = (w & 0x3333333333333333ull) + ((w >> 2) & 0x3333333333333333ull);
		w = (w & 0x0F0F0F0F0F0F0F0Full) + ((w >> 4) & 0x0F0F0F0F0F0F0F0Full);
		w = (w * 0x0101010101010101ull) >> 56;
	#elif (Width == 3)
		z = RealUnitCount - fast_popcount64(
			(w & Zc01_21_03) | ((w >> 1) & Zc01_21_03)
							 | ((w >> 2) & Zc01_21_03));
		w = (w & Bits_20_03) + ((w >> 3) & Bits_20_03) PlusHighestUnit();
		w = (w & Bits_10_06) + ((w >> 6) & Bits_10_06);
		w = (w * Bits_05_01) >> 48 & 255;
	#elif (Width == 4)
		z = (w & 0x3333333333333333ull) | ((w >> 2) & 0x3333333333333333ull);
		z = (z & 0x5555555555555555ull) | ((z >> 1) & 0x5555555555555555ull);
		z = RealUnitCount - fast_popcount64(z);
		w = (w & 0x0F0F0F0F0F0F0F0Full) + ((w >> 4) & 0x0F0F0F0F0F0F0F0Full);
		w = (w * 0x0101010101010101ull) >> 56;
	#elif (Width == 5)
		z = (w & Zc02_12_05) | ((w >> 2) & Zc02_12_05);
		z = (z & Zc01_12_05) | ((z >> 1) & Zc01_12_05)
							 | ((w >> 4) & Zc01_12_05);
		z = RealUnitCount - fast_popcount64(z);
		w = (w & Bits_12_05) + ((w >> 5) & Bits_12_05);
		w = (w * Bits_06_01) >> 50 & 0x1FF;
	#elif (Width == 6)
		z = (w & Zc03_10_06) | ((w >> 3) & Zc03_10_06);
		z = (z & Zc01_10_06) | ((z >> 1) & Zc01_10_06)
							 | ((z >> 2) & Zc01_10_06);
		z = RealUnitCount - fast_popcount64(z);
		w = (w & Bits_10_06) + ((w >> 6) & Bits_10_06);
		w = (w * Bits_05_01) >> 48 & 0x3FF;
	#elif (Width == 7)
		z = (w & Zc03_09_07) | ((w >> 3) & Zc03_09_07);
		z = (z & Zc01_09_07) | ((z >> 1) & Zc01_09_07)
							 | ((z >> 2) & Zc01_09_07);
		z = RealUnitCount - fast_popcount64(z| ((w >> 6) & Zc01_09_07));
		w = (w & Bits_08_07) + ((w >>  7) & Bits_08_07) PlusHighestUnit();
		w = (w & Bits_04_14) + ((w >> 14) & Bits_04_14);
		w = (w & 0x00003FFF) + ((w >> 28) & 0x00003FFF);
	#elif (Width == 8)
		z = (w & 0x0F0F0F0F0F0F0F0Full) | ((w >> 4) & 0x0F0F0F0F0F0F0F0Full);
		z = (z & 0x3333333333333333ull) | ((z >> 2) & 0x3333333333333333ull);
		z = (z & 0x5555555555555555ull) | ((z >> 1) & 0x5555555555555555ull);
		z = RealUnitCount - fast_popcount64(z);
		w = (w & 0x00FF00FF00FF00FFull) + ((w >> 8) & 0x00FF00FF00FF00FFull);
		w = (w * 0x0001000100010001ull) >> 48;
	#elif (Width == 9)
		z = (w & Zc03_07_09) | ((w >> 3) & Zc03_07_09) | ((w >> 6) & Zc03_07_09);
		z = (z & Zc01_07_09) | ((z >> 1) & Zc01_07_09) | ((z >> 2) & Zc01_07_09);
		z = RealUnitCount - fast_popcount64(z);
		w = (w & Bits_06_09) + ((w >> 9) & Bits_06_09) PlusHighestUnit();
		w = (w & 0x00000FFF) + ((w >>18) & 0x00000FFF) + ((w >> 36)/*&0xFFF*/);
	#elif (Width == 10)
		z = !(w & (0x3FFull <<  0));
		if (!(w & (0x3FFull << 10)) && 1 < RealUnitCount) z++;
		if (!(w & (0x3FFull << 20)) && 2 < RealUnitCount) z++;
		if (!(w & (0x3FFull << 30)) && 3 < RealUnitCount) z++;
		if (!(w & (0x3FFull << 40)) && 4 < RealUnitCount) z++;
		if (!(w & (0x3FFull << 50)) && 5 < RealUnitCount) z++;
		w = (w & Bits_06_10) + ((w >> 10) & Bits_06_10);
		w = (w & 0x00000FFF) + ((w >> 20) & 0x00000FFF) + (w >> 40);
	#elif (Width == 11)
		#error "Width == 11 is not supported"
	#elif (Width == 12)
		z = !(w & (0xFFFull <<  0));
		if (!(w & (0xFFFull << 12)) && 1 < RealUnitCount) z++;
		if (!(w & (0xFFFull << 24)) && 2 < RealUnitCount) z++;
		if (!(w & (0xFFFull << 36)) && 3 < RealUnitCount) z++;
		if (!(w & (0xFFFull << 48)) && 4 < RealUnitCount) z++;
		w = (w & 0x000FFF000FFF) + ((w >> 12)  & 0x000FFF000FFF) PlusHighestUnit();
		w = (w & 0x000000FFFFFF) + ((w >> 24)/*& 0x000000FFFFFF*/);
	#elif (Width == 16)
		z = !(w & (0xFFFFull <<  0));
		if (!(w & (0xFFFFull << 16)) && 1 < RealUnitCount) z++;
		if (!(w & (0xFFFFull << 32)) && 2 < RealUnitCount) z++;
		if (!(w & (0xFFFFull << 48)) && 3 < RealUnitCount) z++;
		w = (w & 0x0000FFFF0000FFFF) + ((w >> 16)  & 0x0000FFFF0000FFFF);
		w = (w & 0x00000000FFFFFFFF) + ((w >> 32)/*& 0x00000000FFFFFFFF*/);
	#elif (Width == 20)
		z = !(w & (0xFFFFFull <<  0));
		if (!(w & (0xFFFFFull << 20)) && 1 < RealUnitCount) z++;
		if (!(w & (0xFFFFFull << 40)) && 2 < RealUnitCount) z++;
		w = (w & 0xFFFFF) + ((w >> 20) & 0xFFFFF) PlusHighestUnit();
	#else
		int xxxxWidth = Width; Width "Width" Unsupported
	#endif

		ddsum += w;
		largeRank += z;

  #undef PlusHighestUnit
