#include <iostream>

#include "../include/topk_range_reporter.hpp"

using namespace fulgor;

int main() {
    constexpr uint64_t num_chunks = 6;
    constexpr uint64_t k = 2;
    constexpr uint64_t num_docs = 70 + 1;

    topk_range_reporter topk_rr(k, num_chunks, num_docs);

    std::vector<std::vector<uint32_t>> colors = {{10, 20},        {10, 20, 30, 40, 50},
                                                 {10, 40, 70},    {1, 40, 50, 70},
                                                 {1, 10, 40, 70}, {5, 10, 70}};

    for (uint32_t chunk_id = 0; chunk_id != num_chunks; ++chunk_id) {
        topk_rr.process_doc_ids(chunk_id, colors[chunk_id]);
    }

    topk_rr.finalize();

    /*
        report the topk results for each chunk:
            chunk_id = 0: 10:[0,2] 20:[0,1]
            chunk_id = 1: 40:[1,4] 10:[0,2]
            chunk_id = 2: 40:[1,4] 70:[2,5]
            chunk_id = 3: 40:[1,4] 70:[2,5]
            chunk_id = 4: 40:[1,4] 70:[2,5]
            chunk_id = 5: 70:[2,5] 10:[4,5]
      */
    for (uint32_t chunk_id = 0; chunk_id != num_chunks; ++chunk_id) {
        std::cout << "chunk_id = " << chunk_id << ": ";
        auto const& topk = topk_rr.topk(chunk_id);
        for (auto const& r : topk) {
            std::cout << r.doc_id << ":[" << r.begin << "," << r.end << "] ";
        }
        std::cout << std::endl;
    }

    topk_rr.clear();

    return 0;
}