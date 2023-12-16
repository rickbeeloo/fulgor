#pragma once

#include "../external/sshash/include/dictionary.hpp"
#include "ranked_bit_vector.hpp"
#include "filenames.hpp"
#include "util.hpp"

namespace fulgor {

template <typename ColorClasses>
struct index {
    typedef ColorClasses color_classes_type;

    struct builder;
    struct meta_builder;

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_k2u);
        visitor.visit(m_u2c);
        visitor.visit(m_ccs);
        visitor.visit(m_filenames);
    }

    uint64_t k() const { return m_k2u.k(); }
    uint64_t num_docs() const { return m_ccs.num_docs(); }
    uint64_t num_color_classes() const { return m_ccs.num_color_classes(); }

    typename color_classes_type::iterator_type colors(uint64_t color_class_id) const {
        assert(color_class_id < num_color_classes());
        return m_ccs.colors(color_class_id);
    }

    /* from unitig_id to color_class_id */
    uint64_t u2c(uint64_t unitig_id) const { return m_u2c.rank(unitig_id); }

    void pseudoalign_full_intersection(std::string const& sequence,
                                       std::vector<uint32_t>& results) const; // results = colors?

    void pseudoalign_threshold_union(std::string const& sequence, std::vector<uint32_t>& results,
                                     const double threshold) const;

    void intersect_unitigs(std::vector<uint64_t>& unitig_ids, std::vector<uint32_t>& colors) const;

    uint64_t num_bits() const {
        return m_k2u.num_bits() + m_u2c.bytes() * 8 + m_ccs.num_bits() + m_filenames.num_bits();
    }

    std::string_view filename(uint64_t doc_id) const {
        assert(doc_id < num_docs());
        return m_filenames.filename(doc_id);
    }

    void print_stats() const;

    ColorClasses const& color_classes() const { return m_ccs; }
    sshash::dictionary const& get_dict() const { return m_k2u; }
    ranked_bit_vector const& get_u2c() const { return m_u2c; }
    pthash::bit_vector const& contigs() const { return m_k2u.strings(); }
    filenames const& get_filenames() const { return m_filenames; }

private:
    sshash::dictionary m_k2u;
    ranked_bit_vector m_u2c;
    ColorClasses m_ccs;
    filenames m_filenames;
};

}  // namespace fulgor
