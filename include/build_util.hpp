#pragma once

#include "sketch/hll.h"
#include "kmeans.hpp"

namespace fulgor {

void build_reference_sketches(index_type const& index,
                              uint64_t p,                  // use 2^p bytes per HLL sketch
                              uint64_t num_threads,        // num. threads for construction
                              std::string output_filename  // where the sketches will be serialized
) {
    assert(num_threads > 0);

    const uint64_t num_docs = index.num_docs();
    typename sketch::hll_t::HashType hasher;
    auto const& u2c = index.get_u2c();
    auto const& ccs = index.color_classes();
    const uint64_t num_color_classes = ccs.num_color_classes();
    const uint64_t num_ones = u2c.num_ones();
    assert(num_color_classes == num_ones + 1);

    // std::cout << "num_color_classes = " << num_color_classes << std::endl;
    // std::cout << "num_unitigs = " << u2c.size() << std::endl;

    if (num_ones < num_threads) {
        throw std::runtime_error("there are only " + std::to_string(num_color_classes) +
                                 ": reduce the number of threads.");
    }

    std::vector<std::vector<sketch::hll_t>> thread_sketches(
        num_threads, std::vector<sketch::hll_t>(num_docs, sketch::hll_t(p)));

    struct slice {
        uint64_t begin;                         // start position in u2c
        uint64_t color_id_begin, color_id_end;  // [..)
    };
    std::vector<slice> thread_slices;

    /* compute load */
    uint64_t load = 0;
    {
        uint64_t pop_count = 0;
        uint64_t prev_pos = 0;
        pthash::bit_vector::unary_iterator unary_it(u2c);
        for (uint64_t color_id = 0; color_id != num_color_classes; ++color_id) {
            uint64_t curr_pos = pop_count != num_ones ? unary_it.next() : (u2c.size() - 1);
            uint64_t num_unitigs = curr_pos - prev_pos + 1;
            auto it = ccs.colors(color_id);
            uint64_t size = it.size();
            load += size * num_unitigs;
            pop_count += 1;
            prev_pos = curr_pos + 1;
        }
    }

    const uint64_t load_per_thread = load / num_threads;

    // std::cout << "load " << load << std::endl;
    // std::cout << "load_per_thread " << load_per_thread << std::endl;

    {
        uint64_t prev_pos = 0;
        pthash::bit_vector::unary_iterator unary_it(u2c);
        slice s;
        s.begin = 0;
        s.color_id_begin = 0;
        uint64_t cur_load = 0;

        for (uint64_t color_id = 0; color_id != num_color_classes; ++color_id) {
            uint64_t curr_pos =
                color_id != num_color_classes - 1 ? unary_it.next() : (u2c.size() - 1);

            uint64_t num_unitigs = curr_pos - prev_pos + 1;
            auto it = ccs.colors(color_id);
            uint64_t size = it.size();
            cur_load += size * num_unitigs;
            prev_pos = curr_pos + 1;

            if (cur_load >= load_per_thread or color_id == num_color_classes - 1) {
                s.color_id_end = color_id + 1;

                // std::cout << "slice: begin=" << s.begin << "; color_ids[" << s.color_id_begin <<
                // ","
                //           << s.color_id_end
                //           << "); num. colors = " << (s.color_id_end - s.color_id_begin)
                //           << "; load = " << cur_load << std::endl;

                thread_slices.push_back(s);
                s.begin = prev_pos;
                s.color_id_begin = color_id + 1;
                cur_load = 0;
            }
        }

        assert(thread_slices.size() == num_threads);
    }

    auto exe = [&](uint64_t thread_id) {
        assert(thread_id < thread_slices.size());
        auto& sketches = thread_sketches[thread_id];
        auto s = thread_slices[thread_id];
        uint64_t prev_pos = s.begin;
        std::vector<uint64_t> hashes;
        pthash::bit_vector::unary_iterator unary_it(u2c, s.begin);
        for (uint64_t color_id = s.color_id_begin; color_id != s.color_id_end; ++color_id) {
            uint64_t curr_pos =
                color_id != num_color_classes - 1 ? unary_it.next() : (u2c.size() - 1);
            auto it = ccs.colors(color_id);
            uint64_t size = it.size();
            hashes.reserve(curr_pos - prev_pos + 1);
            for (uint64_t unitig_id = prev_pos; unitig_id <= curr_pos; ++unitig_id) {
                assert(unitig_id < u2c.size());
                assert(index.u2c(unitig_id) == color_id);
                hashes.push_back(hasher.hash(unitig_id));
            }
            for (uint64_t i = 0; i != size; ++i, ++it) {
                uint32_t ref_id = *it;
                assert(ref_id < num_docs);
                for (auto hash : hashes) sketches[ref_id].add(hash);
            }
            prev_pos = curr_pos + 1;
            hashes.clear();
        }
        // essentials::logger("thread-" + std::to_string(thread_id) + " DONE");
    };

    std::vector<std::thread> threads(num_threads);
    for (uint64_t thread_id = 0; thread_id != num_threads; ++thread_id) {
        threads[thread_id] = std::thread(exe, thread_id);
    }
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    /* merge sketches into thread_sketches[0] */
    for (uint64_t i = 0; i != num_docs; ++i) {
        auto& sketch = thread_sketches[0][i];
        for (uint64_t thread_id = 1; thread_id != num_threads; ++thread_id) {
            sketch += thread_sketches[thread_id][i];
        }
    }

    std::ofstream out(output_filename, std::ios::binary);
    if (!out.is_open()) throw std::runtime_error("cannot open file");
    const uint64_t num_bytes = 1ULL << p;
    out.write(reinterpret_cast<char const*>(&num_bytes), 8);
    out.write(reinterpret_cast<char const*>(&num_docs), 8);
    for (auto const& x : thread_sketches[0]) {
        assert(x.m() == num_bytes);
        assert(x.m() == x.core().size());
        uint8_t const* data = x.data();
        out.write(reinterpret_cast<char const*>(data), num_bytes);
    }
    out.close();
}

}  // namespace fulgor