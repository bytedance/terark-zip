public:
const BaseDAWG* get_dawg() const override { return this; }

size_t match_dawg
(MatchContext& ctx, size_t base_nth, fstring str, const OnMatchDAWG& on_match)
const override final {
	return tpl_match_dawg<const OnMatchDAWG&>(ctx, base_nth, str, on_match);
}
size_t match_dawg
(MatchContext& ctx, size_t base_nth, fstring str, const OnMatchDAWG& on_match, const ByteTR& tr)
const override final {
	return tpl_match_dawg<const OnMatchDAWG&, const ByteTR&>(ctx, base_nth, str, on_match, tr);
}
size_t match_dawg
(MatchContext& ctx, size_t base_nth, fstring str, const OnMatchDAWG& on_match, const byte_t* tr)
const override final {
	assert(NULL != tr);
	return tpl_match_dawg<const OnMatchDAWG&>(ctx, base_nth, str, on_match, TableTranslator(tr));
}

struct LongestOnMatch {
	size_t len = 0;
	size_t nth = size_t(-1);
	void operator()(size_t len2, size_t nth2) {
		len = len2;
		nth = nth2;
	}
	bool get_result(size_t* p_len, size_t* p_nth) const {
		if (size_t(-1) != nth) {
			*p_len = len;
			*p_nth = nth;
			return true;
		}
		return false;
	}
};
bool match_dawg_l
(MatchContext& ctx, fstring str, size_t* len, size_t* nth, const ByteTR& tr)
const override final {
	assert(NULL != len);
	assert(NULL != nth);
	LongestOnMatch on_match;
	tpl_match_dawg<LongestOnMatch&, const ByteTR&>(ctx, 0, str, on_match, tr);
	return on_match.get_result(len, nth);
}
bool match_dawg_l
(MatchContext& ctx, fstring str, size_t* len, size_t* nth, const byte_t* tr)
const override final {
	assert(NULL != len);
	assert(NULL != nth);
	assert(NULL != tr);
	LongestOnMatch on_match;
	tpl_match_dawg<LongestOnMatch&>(ctx, 0, str, on_match, TableTranslator(tr));
	return on_match.get_result(len, nth);
}
bool match_dawg_l
(MatchContext& ctx, fstring str, size_t* len, size_t* nth)
const override final {
	assert(NULL != len);
	assert(NULL != nth);
	LongestOnMatch on_match;
	this->tpl_match_dawg<LongestOnMatch&>(ctx, 0, str, on_match);
	return on_match.get_result(len, nth);
}

//-----------------------------------------------------------------------
///@param on_hit(size_t match_end_pos, size_t word_id)
template<class OnMatch>
size_t tpl_match_dawg
(MatchContext& ctx, size_t base_nth, fstring str, OnMatch on_match)
const {
	return this->tpl_match_dawg<OnMatch>(ctx, base_nth, str, on_match, IdentityTR());
}

