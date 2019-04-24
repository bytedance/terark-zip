
public:

struct StateMoveContext {};

transition_t
state_move_fast(size_t parent, auchar_t ch, StateMoveContext)
const {
	return state_move(parent, ch);
}

transition_t
state_move_slow(size_t parent, auchar_t ch, StateMoveContext)
const {
	return state_move(parent, ch);
}


