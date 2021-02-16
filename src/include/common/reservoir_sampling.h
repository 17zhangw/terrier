#include <random>
#include <queue>

namespace noisepage::common {

template <class Value>
class ReservoirSampling {
 public:

  explicit ReservoirSampling(size_t k)
    : limit_(k),
      dist_(0, 1) {}

  void AddSample(const Value *val) {
    double priority = dist_(generator_);
    if (queue_.size() < limit_) {
      queue_.emplace(priority, val);
    } else if (priority > queue_.top().priority_) {
      queue_.pop();
      queue_.emplace(priority, val);
    }
  }

  std::vector<const Value*> GetSamples() {
    std::vector<const Value*> samples;
    samples.reserve(limit_);
    while (!queue_.empty()) {
      samples.emplace_back(queue_.top().value_);
      queue_.pop();
    }
  }

 private:
  template <class ValueKey>
  class Key {
    double priority_;
    const ValueKey *value_;

    Key (double priority, const ValueKey &value)
      : priority_(priority), value_(value) {}
  };

  template <class ValueKey>
  struct KeyCmp {
    constexpr bool operator()(const Key<ValueKey> &lhs, const Key<ValueKey> &rhs) {
      return lhs.priority_ > rhs.priority_;
    }
  };

  size_t limit_;
  std::priority_queue<Key, std::vector<Key>, KeyCmp<ValueKey>> queue_;
  std::mt19937 generator_;
  std::uniform_real_distribution dist_;
};

}  // namespace noisepage::common
