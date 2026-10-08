#ifndef TIMERWHEELS_H
#define TIMERWHEELS_H

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace timerwheels {
	/**
	 * \brief A timer using the "Scheme 4 - Basic Scheme for Timer Intervals within a Specified Range"
	 */

	/**
	 * @brief Minimal move-only type-erased callable for fire bodies.
	 *
	 * @details Unlike `std::function`, this accepts callables that move but
	 * cannot be copied, and subscribing never copies the callable.
	 */
	class move_only_function {
	public:
		move_only_function() noexcept = default;
		move_only_function(move_only_function&&) noexcept = default;
		move_only_function& operator=(move_only_function&&) noexcept = default;
		move_only_function(const move_only_function&) = delete;
		move_only_function& operator=(const move_only_function&) = delete;
		template<class F>
		explicit move_only_function(F&& f)
			: held_(new holder<typename std::decay<F>::type>(std::forward<F>(f))) {}
		void operator()() { held_->call(); }
	private:
		struct holder_base {
			virtual ~holder_base() {}
			virtual void call() = 0;
		};
		template<class F>
		struct holder : holder_base {
			F f;
			template<class G>
			explicit holder(G&& g) : f(std::forward<G>(g)) {}
			void call() override { f(); }
		};
		std::unique_ptr<holder_base> held_;
	};
	template<
		typename DURATION = std::chrono::milliseconds,
		std::size_t RANGE = 1024,
		std::size_t BUCKET = 4,
		typename CLOCK = std::chrono::high_resolution_clock>
	class FixedRangeTimerWheel final {
	public:
		using ticket = std::uint64_t;
	private:
    	static_assert(BUCKET <= RANGE,"The fixed time range must be bigger than a bucket");
    	static_assert(RANGE % BUCKET == 0,"The fixed time range must an integer multiple of the bucket size");

		struct Timer final {
			std::atomic<struct Timer*> next;

			/**
			 * @brief Function to call when the timer elapses.
			 */
			move_only_function fire;

			/**
			 * @brief if non-`DURATION::zero()`, how long to repeat the timer after.
			 */
			DURATION repeat;

			/**
			 * @brief When to schedule.
			 * @description Stored here as we schedule work separately
			 * 
			 */
			DURATION when;

			unsigned remaining;
			bool counted;
			std::uint64_t id;

			Timer(move_only_function&& fire, DURATION&& repeat, DURATION&& when)
				: next(nullptr), fire(std::move(fire)), repeat(std::move(repeat)), when(std::move(when)),
				  remaining(0), counted(false), id(0) {}
		};

		std::array<std::atomic<struct Timer*>, RANGE / BUCKET> wheel;

		/**
		 * @brief Used by the `tick()` thread only; the index in the wheel which is "now".
		 */
		std::size_t index;

		/**
		 * @brief Used by the `tick()` thread only; stores the last time `tick()` was called
		 */
		std::chrono::time_point<CLOCK> last_tick;

		/**
		 * @brief Shared between threads; stores any timers that haven't been added to the wheel.
		 */
		std::atomic<struct Timer*> unscheduled;

		std::uint64_t next_ticket = 1;
		std::unordered_set<ticket> live;
		std::unordered_set<ticket> retired;
		ticket firing = 0;
		mutable std::mutex books;

		// Serializes whole tick() calls so concurrent tickers never advance
		// the wheel twice over the same bucket. Lock order: tick_mu may be
		// held while taking books, never the reverse. tick_mu stays held
		// across user fire bodies (that is what serializes tickers), so a
		// fire body must not call tick() reentrantly; books is the lock that
		// is never held across a fire body.
		std::mutex tick_mu;

		struct Timer* drain_cursor = nullptr;
		std::size_t frontier_ticks_left = 0;
		std::size_t frontier_ticks_total = 0;
		bool frontier_active = false;

		// Call only with books held. Never held across user fire bodies.
		inline void release(struct Timer* timer) {
			live.erase(timer->id);
			retired.erase(timer->id);
			delete timer;
		}

		// unlink timer t from bucket b (head pointer) or the unscheduled list
		inline bool unlink_from(std::atomic<struct Timer*>& head, ticket t) {
			struct Timer* prev = nullptr;
			for (struct Timer* cur = head.load(std::memory_order::memory_order_acquire);
			     cur != nullptr;) {
				struct Timer* nxt = cur->next.load(std::memory_order::memory_order_acquire);
				if (cur->id == t) {
					if (prev == nullptr) {
						struct Timer* expect = cur;
						if (!head.compare_exchange_strong(expect, nxt,
								std::memory_order::memory_order_acq_rel)) {
							return false;
						}
					} else {
						prev->next.store(nxt, std::memory_order::memory_order_release);
					}
					release(cur);
					return true;
				}
				prev = cur;
				cur = nxt;
			}
			return false;
		}

		inline void queue(struct Timer* timer, std::atomic<struct Timer*>& onto) {
		struct Timer* head = nullptr;
		do {
			head = onto.load(std::memory_order::memory_order_acquire);
			timer->next.store(head, std::memory_order::memory_order_relaxed);
		} while (!onto.compare_exchange_weak(head, timer,
				std::memory_order::memory_order_release,
				std::memory_order::memory_order_relaxed));
		}

		inline void integrate_unscheduled() {
			for (
				struct Timer * t = unscheduled.exchange(nullptr, std::memory_order::memory_order_acq_rel), * next = nullptr;
				t != nullptr;
				t = next) {
				next = t->next.load(std::memory_order::memory_order_acquire);
				{
					std::lock_guard<std::mutex> guard(books);
					if (retired.count(t->id) != 0) {
						release(t);
						continue;
					}
				}
				schedule(t, t->when);
			}
		}

		inline void finish_frontier() {
			last_tick += frontier_ticks_total * DURATION(BUCKET);
			frontier_ticks_left = 0;
			frontier_ticks_total = 0;
			frontier_active = false;
			integrate_unscheduled();
		}

		inline bool prepare_due_timer() {
			for (;;) {
				while (drain_cursor != nullptr) {
					std::lock_guard<std::mutex> guard(books);
					if (retired.count(drain_cursor->id) == 0) {
						return true;
					}
					struct Timer* retired_timer = drain_cursor;
					drain_cursor = drain_cursor->next.load(std::memory_order::memory_order_acquire);
					release(retired_timer);
				}

				if (frontier_ticks_left == 0) {
					finish_frontier();
					return false;
				}

				index = (index + 1) % (RANGE / BUCKET);
				--frontier_ticks_left;
				drain_cursor = wheel[index].exchange(nullptr, std::memory_order::memory_order_acq_rel);
			}
		}

		inline void abort_frontier_after_throw() {
			std::lock_guard<std::mutex> guard(books);
			for (struct Timer* rest = drain_cursor; rest != nullptr;) {
				struct Timer* next = rest->next.load(std::memory_order::memory_order_acquire);
				if (retired.count(rest->id) != 0) {
					release(rest);
				} else {
					queue(rest, wheel[(index + 1) % (RANGE / BUCKET)]);
				}
				rest = next;
			}
			drain_cursor = nullptr;
			frontier_ticks_left = 0;
			frontier_ticks_total = 0;
			frontier_active = false;
		}

		/**
		 * @brief insert a timer into the wheel.
		 * @param when how much time remaining before the timer elapses.
		 */
		inline std::size_t bucket_offset(DURATION when) {
			// round up time remaining to the next multiple of BUCKET size;
			// non-positive delays land one bucket ahead so they go off on
			// the next advancing tick instead of waiting out a revolution.
			std::int64_t ticks = static_cast<std::int64_t>(when / DURATION(1));
			std::int64_t per = static_cast<std::int64_t>(DURATION(BUCKET) / DURATION(1));
			std::int64_t steps = (per > 0) ? ((ticks + per - 1) / per) : 1;
			if (steps < 1) {
				steps = 1;
			}

			// fixed time range
			steps = std::min<std::int64_t>(steps, static_cast<std::int64_t>(RANGE / BUCKET));

			return static_cast<std::size_t>(steps);
		}

		inline void schedule(struct Timer* timer, DURATION when) {
			std::size_t pos = (index + bucket_offset(when)) % (RANGE / BUCKET);

			queue(timer, wheel[pos]);
		}

	public:
		struct tick_result {
			std::size_t fired;
			bool more_due;
		};

		FixedRangeTimerWheel() : wheel {}, index(0), last_tick(CLOCK::now()), unscheduled(nullptr) {}

		~FixedRangeTimerWheel() {
			for (struct Timer* cur = drain_cursor, *nxt = nullptr;
			     cur != nullptr; cur = nxt) {
				nxt = cur->next.load(std::memory_order::memory_order_acquire);
				delete cur;
			}
			for (std::size_t b = 0; b < RANGE / BUCKET; ++b) {
				for (struct Timer* cur = wheel[b].exchange(nullptr,
						std::memory_order::memory_order_acq_rel), *nxt = nullptr;
				     cur != nullptr; cur = nxt) {
					nxt = cur->next.load(std::memory_order::memory_order_acquire);
					delete cur;
				}
			}
			for (struct Timer* cur = unscheduled.exchange(nullptr,
					std::memory_order::memory_order_acq_rel), *nxt = nullptr;
			     cur != nullptr; cur = nxt) {
				nxt = cur->next.load(std::memory_order::memory_order_acquire);
				delete cur;
			}
		}

		/**
		 * @brief How many live timers are currently held (each timer counts
		 * once, however many rounds a counted rearm still has). Safe to call
		 * from any thread, including from inside a `fire` body, where it
		 * counts the currently firing timer as live unless it was dropped.
		 */
		std::size_t pending() const {
			std::lock_guard<std::mutex> guard(books);
			return live.size();
		}

		/**
		 * @brief Add a function to be called after a specified delay.
		 * 
		 * @param fire The function to call; always executed on the thread calling `tick()`.
		 * @param when The initial delay before calling the function.
		 * @param repeat If non-zero, the function will be rescheduled immediately after firing after this delay.
		 */
		template<class F>
		ticket schedule(F&& fire, DURATION when, DURATION repeat = DURATION::zero()) {
			struct Timer* timer = new Timer(move_only_function(std::forward<F>(fire)), std::move(repeat), std::move(when));
			{
				std::lock_guard<std::mutex> guard(books);
				timer->id = next_ticket++;
				live.insert(timer->id);
			}
			queue(timer, unscheduled);
			return timer->id;
		}

		/**
		 * @brief Add a function to be called a fixed number of times.
		 *
		 * @param fire The function to call; always executed on the thread calling `tick()`.
		 * @param when The initial delay before the first call.
		 * @param repeat The delay between consecutive calls, measured from each firing.
		 * @param count The total number of calls. Zero schedules nothing.
		 */
		template<class F>
		ticket schedule_n(F&& fire, DURATION when, DURATION repeat, unsigned count) {
			if (count == 0) {
				std::lock_guard<std::mutex> guard(books);
				return next_ticket++;
			}
			struct Timer* timer = new Timer(move_only_function(std::forward<F>(fire)), std::move(repeat), std::move(when));
			timer->counted = true;
			timer->remaining = count;
			{
				std::lock_guard<std::mutex> guard(books);
				timer->id = next_ticket++;
				live.insert(timer->id);
			}
			queue(timer, unscheduled);
			return timer->id;
		}

		/**
		 * @brief Retire one live timer.
		 *
		 * @return true when a live timer was retired; unknown, already-fired
		 * and already-dropped tickets report false. Dropping twice is harmless.
		 * A dropped timer never fires again.
		 */
		bool drop(ticket t) {
			std::lock_guard<std::mutex> guard(books);
			if (live.erase(t) == 0) {
				return false;
			}
			if (t == firing) {
				retired.insert(t);
				return true;
			}
			for (std::size_t b = 0; b < RANGE / BUCKET; ++b) {
				if (unlink_from(wheel[b], t)) {
					return true;
				}
			}
			if (unlink_from(unscheduled, t)) {
				return true;
			}
			// in flight inside the current tick (already unlinked bucket or
			// drain list): silence it when its turn comes.
			retired.insert(t);
			return true;
		}

		/**
		 * @brief Retire every live timer at once.
		 *
		 * @return how many live timers were retired, including a timer
		 * mid-firing on the calling thread. Timers scheduled after this call
		 * returns are unaffected.
		 */
		std::size_t drop_all() {
			std::lock_guard<std::mutex> guard(books);
			std::size_t n = live.size();
			for (ticket t : live) {
				if (t != firing) {
					retired.insert(t);
				}
			}
			live.clear();
			for (std::size_t b = 0; b < RANGE / BUCKET; ++b) {
				for (struct Timer* cur = wheel[b].exchange(nullptr,
						std::memory_order::memory_order_acq_rel), *nxt = nullptr;
				     cur != nullptr; cur = nxt) {
					nxt = cur->next.load(std::memory_order::memory_order_acquire);
					retired.erase(cur->id);
					delete cur;
				}
			}
			for (struct Timer* cur = unscheduled.exchange(nullptr,
					std::memory_order::memory_order_acq_rel), *nxt = nullptr;
			     cur != nullptr; cur = nxt) {
				nxt = cur->next.load(std::memory_order::memory_order_acquire);
				retired.erase(cur->id);
				delete cur;
			}
			if (firing != 0) {
				retired.insert(firing);
			}
			return n;
		}

		/**
		 * @brief Do any outstanding work
		 * @details Fire any timers that have elapsed, and schedule any unscheduled timers.
		 */
		tick_result tick(std::size_t budget) {
			std::lock_guard<std::mutex> tick_guard(tick_mu);
			if (budget == 0) {
				return {0, frontier_active};
			}

			if (!frontier_active) {
				auto now = CLOCK::now();
				frontier_ticks_total = std::min<std::size_t>(
					(now - last_tick) / DURATION(BUCKET), RANGE / BUCKET);
				frontier_ticks_left = frontier_ticks_total;
				frontier_active = frontier_ticks_total != 0;
				if (!frontier_active) {
					integrate_unscheduled();
					return {0, false};
				}
			}

			std::size_t fired_count = 0;
			while (fired_count < budget && prepare_due_timer()) {
				struct Timer* t = drain_cursor;
				drain_cursor = t->next.load(std::memory_order::memory_order_acquire);
				{
					std::lock_guard<std::mutex> guard(books);
					firing = t->id;
				}

				try {
					t->fire();
					++fired_count;
				} catch (...) {
					{
						std::lock_guard<std::mutex> guard(books);
						firing = 0;
						release(t);
					}
					abort_frontier_after_throw();
					throw;
				}

				{
					std::lock_guard<std::mutex> guard(books);
					firing = 0;
					if (retired.count(t->id) != 0) {
						release(t);
						continue;
					}
					if (t->counted) {
						if (--(t->remaining) == 0) {
							release(t);
						} else {
							schedule(t, t->repeat);
						}
					} else if (t->repeat == DURATION::zero()) {
						release(t);
					} else {
						schedule(t, t->repeat);
					}
				}
			}

			bool more = frontier_active && prepare_due_timer();
			return {fired_count, more};
		}

		void tick() {
			(void)tick(std::numeric_limits<std::size_t>::max());
		}
	};
}

#endif
