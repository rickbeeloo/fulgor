#include "index.hpp"
#include "../external/sshash/include/query/streaming_query_canonical_parsing.hpp"

namespace fulgor {

template <typename Iterator>
void intersect(std::vector<Iterator>& iterators, std::vector<uint32_t>& colors) {
    assert(colors.empty());

    if (iterators.empty()) return;

    bool all_very_dense = true;
    for (auto const& i : iterators) {
        if (i.type() != color_classes::hybrid::list_type::complementary_delta_gaps) {
            all_very_dense = false;
            break;
        }
    }

    if (all_very_dense) {
        // step 1: take the union of complementary sets
        std::vector<uint32_t> tmp;

        for (auto& i : iterators) i.reinit_for_complemented_set_iteration();

        uint32_t candidate = (*std::min_element(iterators.begin(), iterators.end(),
                                                [](auto const& x, auto const& y) {
                                                    return x.comp_value() < y.comp_value();
                                                }))
                                 .comp_value();

        const uint32_t num_docs = iterators[0].num_docs();
        tmp.reserve(num_docs);
        while (candidate < num_docs) {
            uint32_t next_candidate = num_docs;
            for (uint64_t i = 0; i != iterators.size(); ++i) {
                if (iterators[i].comp_value() == candidate) iterators[i].next_comp();
                /* compute next minimum */
                if (iterators[i].comp_value() < next_candidate) {
                    next_candidate = iterators[i].comp_value();
                }
            }
            tmp.push_back(candidate);
            assert(next_candidate > candidate);
            candidate = next_candidate;
        }

        // step 2: compute the intersection by scanning tmp
        candidate = 0;
        for (uint32_t i = 0; i != tmp.size(); ++i) {
            while (candidate < tmp[i]) {
                colors.push_back(candidate);
                candidate += 1;
            }
            candidate += 1;  // skip the candidate because it is equal to tmp[i]
        }
        while (candidate < num_docs) {
            colors.push_back(candidate);
            candidate += 1;
        }

        return;
    }

    std::sort(iterators.begin(), iterators.end(),
              [](auto const& x, auto const& y) { return x.size() < y.size(); });

    const uint32_t num_docs = iterators[0].num_docs();
    uint32_t candidate = iterators[0].value();
    uint64_t i = 1;
    while (candidate < num_docs) {
        for (; i != iterators.size(); ++i) {
            iterators[i].next_geq(candidate);
            uint32_t val = iterators[i].value();
            if (val != candidate) {
                candidate = val;
                i = 0;
                break;
            }
        }
        if (i == iterators.size()) {
            colors.push_back(candidate);
            iterators[0].next();
            candidate = iterators[0].value();
            i = 1;
        }
    }
}

void stream_through(sshash::dictionary const& k2u, std::string const& sequence,
                    std::vector<uint64_t>& unitig_ids) {
    sshash::streaming_query_canonical_parsing query(&k2u);
    query.start();
    const uint64_t num_kmers = sequence.length() - k2u.k() + 1;
    for (uint64_t i = 0, prev_unitig_id = -1; i != num_kmers; ++i) {
        char const* kmer = sequence.data() + i;
        auto answer = query.lookup_advanced(kmer);
        if (answer.kmer_id != sshash::constants::invalid_uint64) {  // kmer is positive
            if (answer.contig_id != prev_unitig_id) {
                unitig_ids.push_back(answer.contig_id);
                prev_unitig_id = answer.contig_id;
            }
        }
    }
}

template <typename ColorClasses>
void index<ColorClasses>::pseudoalign_full_intersection(std::string const& sequence,
                                                        std::vector<uint32_t>& colors) const {
    if (sequence.length() < m_k2u.k()) return;
    colors.clear();
    std::vector<uint64_t> unitig_ids;
    stream_through(m_k2u, sequence, unitig_ids);
    intersect_unitigs(unitig_ids, colors);
}

template <typename ColorClasses>
void index<ColorClasses>::intersect_unitigs(std::vector<uint64_t>& unitig_ids,
                                            std::vector<uint32_t>& colors) const {
    std::vector<uint64_t> color_class_ids;
    std::vector<typename ColorClasses::iterator_type> iterators;

    /* deduplicate unitig_ids */
    std::sort(unitig_ids.begin(), unitig_ids.end());
    auto end = std::unique(unitig_ids.begin(), unitig_ids.end());
    color_class_ids.reserve(end - unitig_ids.begin());
    for (auto it = unitig_ids.begin(); it != end; ++it) {
        uint64_t unitig_id = *it;
        uint64_t color_class_id = u2c(unitig_id);
        color_class_ids.push_back(color_class_id);
    }

    /* deduplicate color_class_ids */
    std::sort(color_class_ids.begin(), color_class_ids.end());
    end = std::unique(color_class_ids.begin(), color_class_ids.end());
    iterators.reserve(end - color_class_ids.begin());
    for (auto it = color_class_ids.begin(); it != end; ++it) {
        uint64_t color_class_id = *it;
        auto fwd_it = m_ccs.colors(color_class_id);
        iterators.push_back(fwd_it);
    }

    intersect(iterators, colors);
}

}  // namespace fulgor
