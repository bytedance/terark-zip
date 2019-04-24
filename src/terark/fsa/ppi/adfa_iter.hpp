template<class CharT>
class MyLexIteratorT : public ADFA_LexIteratorData<CharT> {
	typedef ADFA_LexIteratorData<CharT> super;
	using super::m_ctx;
	using super::m_dfa;
	using super::m_root;
	using super::m_word;
	using super::m_iter;
	using super::m_node;
	using super::m_curr;
	using super::m_isForward;
	typedef typename super::Layer Layer;

	void mark_word_end_zero_at(size_t curr) {
		m_curr = curr;
		m_word.push_back('\0');
		m_word.pop_back();
	}

public:
	typedef basic_fstring<CharT> fstr;
	typedef typename fstr::uc_t  uc_t;

	MyLexIteratorT(const MyType* dfa) : super(dfa) {}

	size_t calc_word_len() const {
#if 0
		size_t len = 0;
		for (size_t pos = 0; pos < m_iter.size(); ++pos)
			len += m_iter[pos].zlen;
		len += m_iter.size() - 1;
		return len;
#else
		return m_word.size();
#endif
	}

#ifdef __INTEL_COMPILER
	virtual
#endif
	bool incr() override final {
		if (m_iter.empty()) {
			//THROW_STD(out_of_range, "m_iter is empty, must seek first");
			return false;
		}
		if (!m_isForward) {
			m_isForward = true;
			for (size_t pos = 0; pos < m_iter.size(); ++pos) {
				if (m_iter[pos].iter < m_iter[pos].size)
					m_iter[pos].iter++;
			}
		}
		assert(calc_word_len() == m_word.size());
		size_t top = m_iter.size();
		size_t all = m_node.size();
		size_t len = m_word.size();
		while (top) {
			--top;
			if (m_iter[top].iter == m_iter[top].size) {
				if (0 == top) {
					m_iter.erase_all();
					return false;
				}
			//	int is_not_root = top ? 1 : 0;
				int is_not_root = 1;
				assert(all >= (size_t)m_iter[top].size);
				assert(len >= (size_t)m_iter[top].zlen + is_not_root);
				all -= m_iter[top].size;
				len -= m_iter[top].zlen + is_not_root;
			}
			else {
				assert(m_iter[top].iter >= 0);
				assert(m_iter[top].iter < m_iter[top].size);
				break;
			}
		}
		MyType const* dfa = static_cast<const MyType*>(m_dfa);
		size_t dsigma = dfa->get_sigma();
		m_node.risk_set_size(all);
		m_word.risk_set_size(len);
		m_iter.risk_set_size(top + 1);
		assert(m_iter[top].iter < m_iter[top].size);
		assert(calc_word_len() == len);
		size_t itop = all - m_iter[top].size + m_iter[top].iter;
		m_iter[top].iter++;

		for (;;) {
			size_t oldsize = m_node.size(); assert(itop < oldsize);
			size_t curr = m_node[itop].target;
			m_node.resize_no_init(oldsize + dsigma);
			Layer la;
			la.parent = curr;
			la.size = dfa->get_all_move(curr, &m_node[oldsize]);
			m_node.risk_set_size(oldsize + la.size);
			assert(calc_word_len() == m_word.size());
			m_word.push_back(m_node[itop].ch);
			if (dfa->is_pzip(curr)) {
				fstring zs = dfa->get_zpath_data(curr, &m_ctx);
				la.zlen = zs.size();
				m_word.append(zs.data(), zs.size());
			}
			else {
				la.zlen = 0;
			}
			if (dfa->is_term(curr)) {
				la.iter = 0;
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
				mark_word_end_zero_at(curr);
				return true;
			}
			else if (la.size) {
				la.iter = 1;
				itop = oldsize;
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
			}
			else {
				assert(0);
				THROW_STD(runtime_error, "Bad DFA: Non-term node has no children");
			}
		}
		assert(0);
		return true;
	}

	virtual bool decr() override {
		if (m_iter.empty()) {
			//THROW_STD(out_of_range, "m_iter is empty, must seek first");
			return false;
		}
		assert(calc_word_len() == m_word.size());
		if (m_isForward) {
			m_isForward = false;
			for (size_t pos = 0; pos < m_iter.size(); ++pos) m_iter[pos].iter--;
		}
		MyType const* dfa = static_cast<const MyType*>(m_dfa);
		size_t top = m_iter.size();
		size_t all = m_node.size();
		size_t len = m_word.size();
		while (top) {
			--top;
			if (m_iter[top].iter == 0) {
				m_iter[top].iter = -1;
				size_t curr = m_iter[top].parent;
				if (dfa->is_term(curr)) {
					m_node.risk_set_size(all);
					m_iter.risk_set_size(top+1);
					m_word.risk_set_size(len);
					mark_word_end_zero_at(curr);
					return true;
				}
			}
			if (m_iter[top].iter == -1) {
				if (0 == top) {
					m_iter.erase_all();
					return false;
				}
			//	int is_not_root = top ? 1 : 0;
				int is_not_root = 1;
				assert(all >= (size_t)m_iter[top].size);
				assert(len >= (size_t)m_iter[top].zlen + is_not_root);
				all -= m_iter[top].size;
				len -= m_iter[top].zlen + is_not_root;
			}
			else {
				assert(m_iter[top].iter >= 0);
				assert(m_iter[top].iter <= m_iter[top].size);
				break;
			}
		}
		size_t dsigma = dfa->get_sigma();
		m_node.risk_set_size(all);
		m_word.risk_set_size(len);
		m_iter.risk_set_size(top + 1);
		assert(m_iter[top].iter > 0);
		assert(m_iter[top].iter <= m_iter[top].size);
		assert(calc_word_len() == len);
		size_t itop = all - m_iter[top].size + --m_iter[top].iter;

		for (;;) {
			size_t oldsize = m_node.size(); assert(itop < oldsize);
			size_t curr = m_node[itop].target;
			m_node.resize_no_init(oldsize + dsigma);
			Layer la;
			la.parent = curr;
			la.size = dfa->get_all_move(curr, &m_node[oldsize]);
			m_node.risk_set_size(oldsize + la.size);
			assert(calc_word_len() == m_word.size());
			m_word.push_back(m_node[itop].ch);
			if (dfa->is_pzip(curr)) {
				fstring zs = dfa->get_zpath_data(curr, &m_ctx);
				la.zlen = zs.size();
				m_word.append(zs.data(), zs.size());
			}
			else {
				la.zlen = 0;
			}
			la.iter = la.size - 1;
			m_iter.push_back(la);
			assert(calc_word_len() == m_word.size());
			if (la.size) {
				itop = oldsize + la.size - 1;
			}
			else {
				assert(dfa->is_term(curr)); // leaf must be a term
				mark_word_end_zero_at(curr);
				return true;
			}
		}
		assert(0);
		return true;
	}

	virtual bool seek_end() override {
		MyType const* dfa = static_cast<const MyType*>(m_dfa);
		reset1(m_root);
		m_isForward = false;
		Layer la;
		size_t dsigma = dfa->get_sigma();
		size_t oldsize = m_node.size();
		m_node.resize_no_init(oldsize + dsigma);
		la.parent = m_root;
		la.size = dfa->get_all_move(m_root, &m_node[oldsize]);
		la.iter = la.size;
		m_node.risk_set_size(oldsize + la.size);
		if (dfa->is_pzip(m_root)) {
			fstring zs = dfa->get_zpath_data(m_root, &m_ctx);
			m_word.append(zs.data(), zs.size());
			la.zlen = zs.size();
		} else {
			la.zlen = 0;
		}
		m_iter.push_back(la);
		return decr();
	}

	virtual bool seek_lower_bound(fstr key) override {
		const MyType* dfa = static_cast<const MyType*>(m_dfa);
		return set_to_lower_bound(dfa, m_root, key, IdentityTR());
	}

	// after calling this function, must call incr:
	// ret = iter->seek_lower_bound("somekey");
	// while (ret) {
	//     fstring word = iter->word();
	//     printf("%.*s\n", word.ilen(), word.data());
	//     ret = iter->incr();
	// }
	template<class TR>
	inline bool
	set_to_lower_bound(const MyType* dfa, size_t root, fstr key, TR tr) {
		assert(dfa == m_dfa);
		reset1(root);
		m_isForward = true;
		size_t dsigma = dfa->get_sigma();
		size_t curr = root;

		for (size_t pos = 0; ; ++pos) {
			Layer  la;
			size_t oldsize = m_node.size();
			m_node.resize_no_init(oldsize + dsigma);
			la.parent = curr;
			la.size = dfa->get_all_move(curr, &m_node[oldsize]);
			m_node.risk_set_size(oldsize + la.size);
			if (dfa->is_pzip(curr)) {
				fstring zs = dfa->get_zpath_data(curr, &m_ctx);
				size_t nk = key.size() - pos;
				size_t n = std::min(nk, zs.size());
				m_word.append(zs.data(), zs.size());
				la.zlen = zs.size();
			//	fprintf(stderr, "m_word.appends(%.*s)\n", la.zlen, zs.data());
				for (size_t j = 0; j < n; ++pos, ++j) {
					byte_t c = (byte_t)tr((byte_t)key.p[pos]);
					if (c < zs[j]) { // OK, current word is lower_bound
						la.iter = 0;
						m_iter.push_back(la);
						mark_word_end_zero_at(curr);
						if (dfa->is_term(curr)) return true;
						else return incr();
					}
					else if (c > zs[j]) { // next word is lower_bound
						la.iter = la.size;
						m_iter.push_back(la);
						assert(calc_word_len() == m_word.size());
						return incr();
					}
				}
				assert(pos <= key.size());
				if (nk <= zs.size()) {
					// OK, current word this is lower_bound
					la.iter = 0;
					m_iter.push_back(la);
					mark_word_end_zero_at(curr);
					if (dfa->is_term(curr)) return true;
					else return incr();
				}
			}
			else {
				assert(!dfa->is_pzip(curr));
				assert(pos <= key.size());
				la.zlen = 0;
				if (key.size() == pos) { // done
					la.iter = 0;
					m_iter.push_back(la);
					mark_word_end_zero_at(curr);
					if (dfa->is_term(curr)) return true;
					else return incr();
				}
			}
			if (0 == la.size) {
				la.iter = 0;
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
			//	assert(dfa->is_term(curr)); // empty dfa makes this assert fail
				mark_word_end_zero_at(curr);
				if (m_word.size() == key.size()) {
					if (dfa->is_term(curr))
						return true;
					else
						return incr();
				}
				else if (m_word.size() < key.size())
					return incr();
				else return false;
			}
			uc_t uch = (uc_t)tr(key[pos]);
			auto beg = &m_node[oldsize];
			auto end = beg + la.size;
			auto low = lower_bound_ex(beg, end, uch, CharTarget_By_ch());
			if (low < end && uch == low->ch) {
				la.iter = (int)(low - beg + 1);
				m_iter.push_back(la);
				m_word.push_back(uch);
			//	fprintf(stderr, "m_word.appendc(%c)\n", uch);
				curr = low->target;
			}
			else {
				la.iter = (int)(low - beg);
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
				return incr();
			}
		}
		assert(0);
	}

	virtual void reset(const BaseDFA* dfa, size_t root) override {
		assert(nullptr != dfa);
		if (dfa != m_dfa && dynamic_cast<const MyType*>(dfa) == nullptr) {
			THROW_STD(invalid_argument, "dfa instance is not the expected class");
		}
		assert(root < dfa->v_total_states());
		m_dfa = dfa;
		m_isForward = true;
		reset1(root);
	}

	void reset1(size_t root) {
		m_root = root;
		m_curr = size_t(-1);
		m_word.risk_set_size(0);
		m_iter.risk_set_size(0);
		m_node.risk_set_size(0);
		m_ctx.reset();
	}

	virtual size_t seek_max_prefix(fstr key) override {
		const MyType* dfa = static_cast<const MyType*>(m_dfa);
		return seek_max_prefix_impl(dfa, key, IdentityTR());
	}

	template<class TR>
	size_t seek_max_prefix_impl(const MyType* dfa, fstr key, TR tr) {
		reset1(initial_state);
		m_isForward = true;
		size_t oldsize = 0;
		size_t dsigma = dfa->get_sigma();
		size_t curr = initial_state;
		size_t pos = 0;
		Layer  la;

		for ( ; ; ++pos) {
			oldsize = m_node.size();
			m_node.resize_no_init(oldsize + dsigma);
			la.parent = curr;
			la.size = dfa->get_all_move(curr, &m_node[oldsize]);
			m_node.risk_set_size(oldsize + la.size);
			if (dfa->is_pzip(curr)) {
				fstring zs = dfa->get_zpath_data(curr, &m_ctx);
				size_t nk = key.size() - pos;
				size_t n = std::min(nk, zs.size());
				la.zlen = zs.size();
				m_word.append(zs.data(), zs.size());
			//	fprintf(stderr, "m_word.appends(%.*s)\n", la.zlen, zs.data());
				for(size_t j = 0; j < n; ++pos, ++j) {
					byte_t c = (byte_t)tr((byte_t)key.p[pos]);
					if (c != zs[j]) { // OK, current word has max matching prefix
						goto ZS_MatchDone;
					}
				}
				assert(pos <= key.size());
				if (nk <= zs.size()) { // OK, current word has max matching prefix
					goto ZS_MatchDone;
				}
			}
			else {
				assert(pos <= key.size());
				la.zlen = 0;
				if (key.size() == pos) { // done
			ZS_MatchDone:
					mark_word_end_zero_at(curr);
					if (dfa->is_term(curr)) {
						la.iter = 0;
						m_iter.push_back(la);
						return pos;
					}
					if (0 == la.size) {
						la.iter = 0;
						m_iter.push_back(la);
						assert(calc_word_len() == m_word.size());
						assert(dfa->is_term(curr));
						return pos;
					}
					break;
				}
			}
			if (0 == la.size) {
				la.iter = 0;
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
				assert(dfa->is_term(curr));
				mark_word_end_zero_at(curr);
				return pos;
			}
			uc_t uch = (uc_t)tr(key[pos]);
			auto beg = &m_node[oldsize];
			auto low = lower_bound_ex_0(beg, la.size, uch, CharTarget_By_ch());
			if (low < size_t(la.size) && uch == beg[low].ch) {
				la.iter = (short)(low + 1);
				m_iter.push_back(la);
				m_word.push_back(uch);
			//	fprintf(stderr, "m_word.appendc(%c)\n", uch);
				curr = beg[low].target;
			} else {
				break;
			}
		}
		assert(la.size > 0);
		la.iter = 1;
		m_iter.push_back(la);
		assert(calc_word_len() == m_word.size());
		m_word.push_back((uc_t)m_node[oldsize].ch);
		curr = m_node[oldsize].target;
		for (;;) {
			assert(1 == m_iter.back().iter);
			oldsize = m_node.size();
			m_node.resize_no_init(oldsize + dsigma);
			la.parent = curr;
			la.size = dfa->get_all_move(curr, &m_node[oldsize]);
			m_node.risk_set_size(oldsize + la.size);
			if (dfa->is_pzip(curr)) {
				fstring zs = dfa->get_zpath_data(curr, &m_ctx);
				m_word.append(zs.data(), zs.size());
				la.zlen = (short)zs.size();
			} else {
				la.zlen = 0;
			}
			if (0 == la.size) {
				la.iter = 0;
				m_iter.push_back(la);
				assert(calc_word_len() == m_word.size());
				assert(dfa->is_term(curr));
				mark_word_end_zero_at(curr);
				break;
			}
			la.iter = 1;
			m_iter.push_back(la);
			assert(calc_word_len() == m_word.size());
			CharTarget<size_t> ct = m_node[oldsize];
			m_word.push_back((byte_t)ct.ch);
			curr = ct.target;
		}
		return pos;
	}
};

#if !defined(TERARK_FSA_has_override_adfa_make_iter)
virtual ADFA_LexIterator* adfa_make_iter(size_t root = initial_state)
const override {
	return new MyLexIteratorT<char>(this);
}

virtual ADFA_LexIterator16* adfa_make_iter16(size_t root = initial_state)
const override {
	return new MyLexIteratorT<uint16_t>(this);
}
#endif
