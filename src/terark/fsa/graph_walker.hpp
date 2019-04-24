#ifndef __terark_automata_graph_walker_hpp__
#define __terark_automata_graph_walker_hpp__

#include <boost/current_function.hpp>
#include <boost/noncopyable.hpp>
#include <terark/bitmap.hpp>
#include "fsa.hpp"

namespace terark {

#define ASSERT_isNotFree(s)       assert(!this->is_free(s))
#define ASSERT_isNotFree2(au,s)   assert(!au->is_free(s))

template<class VertexID>
class BFS_GraphWalker {
protected:
	size_t _M_depth = 0;
	size_t idx = 0;
	valvec<VertexID> q1, q2;
	febitvec     color;
public:
	void resize(size_t VertexNum) {
		assert(q1.size() == idx);
		assert(q2.empty());
		q1.reserve(std::min<size_t>(VertexNum, 512));
		q2.reserve(std::min<size_t>(VertexNum, 512));
		q1.erase_all();
		idx = 0;
		_M_depth = 0;
		color.resize_no_init(VertexNum);
		color.fill(0);
	}
	void putRoot(VertexID root) {
		assert(root < color.size());
		q1.push_back(root);
		color.set1(root);
	}
	VertexID next() {
		assert(idx <= q1.size());
		if (q1.size() == idx) {
			q1.swap(q2);
			idx = 0;
			_M_depth++;
			q2.erase_all();
		}
		assert(!q1.empty());
		return q1[idx++];
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i];
			if (this->color.is0(child)) {
				this->q2.push_back(child);
				this->color.set1(child);
			}
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i].target;
			if (this->color.is0(child)) {
				this->q2.push_back(child);
				this->color.set1(child);
			}
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					this->q2.push_back(child);
					this->color.set1(child);
				}
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					if (on_edge(parent, child, true)) {
						this->q2.push_back(child);
						this->color.set1(child);
					}
				} else
					on_edge(parent, child, false);
			});
	}
	bool is_finished() const { return q1.size() + q2.size() == idx; }
	size_t depth() const { return _M_depth; }
};

template<class VertexID, class ColorID = VertexID>
class BFS_MultiPassGraphWalker {
protected:
	size_t  _M_depth = 0;
	size_t  idx = 0;
	ColorID color_id = 0;
	valvec<VertexID> q1, q2;
	valvec< ColorID> color;
public:
	void resize(size_t VertexNum) {
		assert(q1.size() == idx);
		assert(q2.empty());
		q1.reserve(std::min<size_t>(VertexNum, 512));
		q2.reserve(std::min<size_t>(VertexNum, 512));
		q1.erase_all();
		idx = 0;
		_M_depth = 0;
		color.resize(VertexNum);
		color_id++;
	}
	void putRoot(VertexID root) {
		assert(root < color.size());
		color[root] = color_id;
		q1.push_back(root);
	}
	VertexID next() {
		assert(idx <= q1.size());
		if (q1.size() == idx) {
			q1.swap(q2);
			idx = 0;
			_M_depth++;
			q2.erase_all();
		}
		assert(!q1.empty());
		return q1[idx++];
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i];
			assert(this->color[child] <= color_id);
			if (this->color[child] < color_id) {
				this->color[child] = color_id;
				this->q2.push_back(child);
			}
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i].target;
			assert(this->color[child] <= color_id);
			if (this->color[child] < color_id) {
				this->color[child] = color_id;
				this->q2.push_back(child);
			}
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				assert(this->color[child] <= color_id);
				if (this->color[child] < color_id) {
					this->color[child] = color_id;
					this->q2.push_back(child);
				}
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				assert(this->color[child] <= color_id);
				if (this->color[child] < color_id) {
					if (on_edge(parent, child, true)) {
						this->color[child] = color_id;
						this->q2.push_back(child);
					}
				} else
					on_edge(parent, child, false);
			});
	}
	bool is_finished() const { return q1.size() + q2.size() == idx; }
	size_t depth() const { return _M_depth; }
};

template<class VertexID>
class BFS_TreeWalker {
protected:
	size_t _M_depth = 0;
	size_t idx = 0;
	valvec<VertexID> q1, q2;
public:
	void resize(size_t VertexNum) {
		assert(q1.size() == idx);
		assert(q2.empty());
		q1.reserve(std::min<size_t>(VertexNum, 512));
		q2.reserve(std::min<size_t>(VertexNum, 512));
		q1.erase_all();
		idx = 0;
		_M_depth = 0;
	}
	void putRoot(VertexID root) {
		q1.push_back(root);
	}
	VertexID next() {
		assert(idx <= q1.size());
		if (q1.size() == idx) {
			q1.swap(q2);
			idx = 0;
			_M_depth++;
			q2.erase_all();
		}
		assert(!q1.empty());
		return q1[idx++];
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i];
			this->q2.push_back(child);
		};
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i].target;
			this->q2.push_back(child);
		};
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				this->q2.push_back(child);
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (on_edge(parent, child, true)) {
					this->q2.push_back(child);
				}
			});
	}
	bool is_finished() const { return q1.size() + q2.size() == idx; }
	size_t depth() const { return _M_depth; }
};


/// The Fastest Graph Walker: similar with DFS and BFS,
/// but neither of them!
/// PFS is: Performance First Search
template<class VertexID>
class PFS_GraphWalker {
protected:
	valvec<VertexID> stack;
	febitvec     color;
public:
	void resize(size_t VertexNum) {
		assert(stack.empty());
		color.resize_no_init(VertexNum);
		color.fill(0);
		stack.reserve(512);
	}
	void putRoot(VertexID root) {
		assert(root < color.size());
		stack.push_back(root);
		color.set1(root);
	}
	VertexID next() {
		assert(!stack.empty());
		VertexID x = stack.back();
		stack.pop_back();
		return x;
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i];
			if (this->color.is0(child)) {
				this->stack.push_back(child);
				this->color.set1(child);
			}
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i].target;
			if (this->color.is0(child)) {
				this->stack.push_back(child);
				this->color.set1(child);
			}
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					this->stack.push_back(child);
					this->color.set1(child);
				}
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					if (on_edge(parent, child, true)) {
						this->stack.push_back(child);
						this->color.set1(child);
					}
				} else
					on_edge(parent, child, false);
			});
	}
	bool is_finished() const { return stack.empty(); }
};

template<class VertexID>
class DFS_GraphWalker { // in DFS Preorder
protected:
	valvec<VertexID> stack;
	febitvec     color;
public:
	void resize(size_t VertexNum) {
		assert(stack.empty());
		color.resize_no_init(VertexNum);
		color.fill(0);
		stack.reserve(512);
	}
	void putRoot(VertexID root) {
		assert(root < color.size());
		stack.push_back(root);
		// don't call color.set1(root)
	}
	VertexID next() {
		assert(!stack.empty());
		VertexID x = stack.back();
		assert(color.is0(x));
		color.set1(x);
		do stack.pop_back(); // skip all visited states in stack
		while (!stack.empty() && color.is1(stack.back()));
		return x;
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i];
			if (this->color.is0(child)) {
				this->stack.push_back(child);
				// Don't call color.set1(child) here!
				// so there may have multiple copy of child in stack
			}
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i].target;
			if (this->color.is0(child)) {
				this->stack.push_back(child);
				// Don't call color.set1(child) here!
				// so there may have multiple copy of child in stack
			}
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					this->stack.push_back(child);
					// Don't call color.set1(child) here!
					// so there may have multiple copy of child in stack
				}
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color.is0(child)) {
					if (on_edge(parent, child, true)) {
						this->stack.push_back(child);
						// Don't call color.set1(child) here!
						// so there may have multiple copy of child in stack
					}
				} else
					on_edge(parent, child, false);
			});
	}
	bool is_finished() const { return stack.empty(); }
};

template<class VertexID, class ColorID = VertexID>
class DFS_MultiPassGraphWalker { // in DFS Preorder
protected:
	valvec<VertexID> stack;
	valvec< ColorID> color;
	ColorID          color_id = 0;
public:
	void resize(size_t VertexNum) {
		assert(stack.empty());
		stack.reserve(512);
		color.resize(VertexNum);
		color_id++;
		// don't set color[root] = color_id;
	}
	void putRoot(VertexID root) {
		assert(root < color.size());
		stack.push_back(root);
		// don't call color.set1(root)
	}
	VertexID next() { // in DFS Preorder
		assert(!stack.empty());
		VertexID x = stack.back();
		assert(color[x] < color_id);
		color[x] = color_id;
		do stack.pop_back(); // skip all visited states in stack
		while (!stack.empty() && color[stack.back()] == color_id);
		return x;
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i];
			if (this->color[child] < color_id) {
				this->stack.push_back(child);
				// Don't set color[child] = color_id here!
				// so there may have multiple copy of child in stack
			}
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i].target;
			if (this->color[child] < color_id) {
				this->stack.push_back(child);
				// Don't set color[child] = color_id here!
				// so there may have multiple copy of child in stack
			}
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color[child] < color_id) {
					this->stack.push_back(child);
					// Don't set color[child] = color_id here!
					// so there may have multiple copy of child in stack
				}
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (this->color[child] < color_id) {
					if (on_edge(parent, child, true)) {
						this->stack.push_back(child);
						// Don't set color[child] = color_id here!
						// so there may have multiple copy of child in stack
					}
				} else
					on_edge(parent, child, false);
			});
	}
	bool is_finished() const { return stack.empty(); }
};

template<class VertexID>
class DFS_TreeWalker { // in DFS Preorder
protected:
	valvec<VertexID> stack;
public:
	void resize(size_t /*VertexNum*/) {
		stack.reserve(512);
	}
	void putRoot(VertexID root) {
		stack.push_back(root);
	}
	VertexID next() {
		assert(!stack.empty());
		VertexID x = stack.back();
		stack.pop_back();
		return x;
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i];
			this->stack.push_back(child);
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		for(size_t i = n_children; i > 0; ) {
			size_t child = children[--i].target;
			this->stack.push_back(child);
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				this->stack.push_back(child);
			});
	}
	template<class Graph, class OnEdge>
	void putChildren2(const Graph* g, VertexID parent, OnEdge on_edge) {
		assert(parent < GraphTotalVertexes(g));
		g->for_each_dest_rev(parent,
			[&,parent](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				if (on_edge(parent, child, true)) {
					this->stack.push_back(child);
				}
			});
	}
	bool is_finished() const { return stack.empty(); }
};

// C of CFS means:
//   1. C == (B + D) / 2
//   2. Cache friendly
// First 2 level in BFS order, others in DFS order
template<class VertexID>
class CFS_TreeWalker {
protected:
	valvec<VertexID> m_q1, m_q2;
	size_t m_depth;
	size_t m_q1_pos;
	static const size_t MaxBFS_Depth = 2;
public:
	void resize(size_t /*VertexNum*/) {
		m_q1.erase_all(); m_q1.reserve(512);
		m_q2.erase_all(); m_q2.reserve(512);
		m_depth = 0;
		m_q1_pos = 0;
	}
	void putRoot(VertexID root) {
		assert(0 == m_depth);
		m_q1.push_back(root);
	}
	VertexID next() {
		assert(m_q1.size() + m_q2.size() > 0);
		if (m_depth < MaxBFS_Depth) {
			assert(m_q1_pos <= m_q1.size());
			if (m_q1_pos < m_q1.size()) {
				return m_q1[m_q1_pos++];
			}
			else {
				m_depth++;
				m_q1.erase_all();
				if (m_depth < MaxBFS_Depth) {
					m_q1.swap(m_q2);
					m_q1_pos = 1;
					return m_q1[0];
				} else {
					std::reverse(m_q2.begin(), m_q2.end());
					return m_q2.pop_val();
				}
			}
		}
		else {
			return m_q2.pop_val();
		}
	}
	void putChildrenT(const size_t* children, size_t n_children) {
		size_t oldsize = m_q2.size();
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i];
			this->m_q2.push_back(child);
		}
		if (m_depth >= MaxBFS_Depth) {
			std::reverse(m_q2.begin() + oldsize, m_q2.end());
		}
	}
	template<class T>
	void putChildrenT(const T* children, size_t n_children) {
		size_t oldsize = m_q2.size();
		for(size_t i = 0; i < n_children; ++i) {
			size_t child = children[i].target;
			this->m_q2.push_back(child);
		}
		if (m_depth >= MaxBFS_Depth) {
			std::reverse(m_q2.begin() + oldsize, m_q2.end());
		}
	}
	template<class Graph>
	void putChildren(const Graph* g, VertexID parent) {
		assert(parent < GraphTotalVertexes(g));
		size_t oldsize = m_q2.size();
		g->for_each_dest(parent,
			[this,g](VertexID child) {
				assert(child < GraphTotalVertexes(g));
				ASSERT_isNotFree2(g, child);
				this->m_q2.push_back(child);
			});
		if (m_depth >= MaxBFS_Depth) {
			std::reverse(m_q2.begin() + oldsize, m_q2.end());
		}
	}
	bool is_finished() const {
		if (m_depth < MaxBFS_Depth) {
			assert(m_q1_pos <= m_q1.size() + m_q2.size());
			return m_q1_pos >= m_q1.size() + m_q2.size();
		} else {
			return m_q2.empty();
		}
	}
};

} // namespace terark

#endif // __terark_automata_graph_walker_hpp__


