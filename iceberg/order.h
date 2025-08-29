#pragma once

#include <concepts>
#include <map>
#include <set>
#include <vector>

namespace iceberg {

using DeleteId = uint32_t;
using SetId = uint32_t;

inline std::vector<std::vector<SetId>> OptimizeDeletes(const std::vector<std::set<DeleteId>>& delete_sets) {
  size_t max_delete_set_size = [&delete_sets]() {
    size_t result = 0;
    for (const auto& delete_set : delete_sets) {
      result = std::max(delete_set.size(), result);
    }
    return result;
  }();

  std::vector<std::vector<SetId>> delete_sets_grouped_by_size(max_delete_set_size + 1);
  for (SetId set_id = 0; set_id < delete_sets.size(); ++set_id) {
    delete_sets_grouped_by_size[delete_sets[set_id].size()].emplace_back(set_id);
  }

  using Chain = std::vector<SetId>;
  using ChainId = uint32_t;

  std::vector<Chain> chains;
  std::map<DeleteId, ChainId> delete_id_to_chain_id;

  for (size_t set_size = 0; set_size <= max_delete_set_size; ++set_size) {
    for (SetId set_id : delete_sets_grouped_by_size[set_size]) {
      const std::set<DeleteId>& current_set = delete_sets[set_id];

      bool continues_some_chain = false;

      for (DeleteId delete_id : current_set) {
        if (!delete_id_to_chain_id.contains(delete_id)) {
          continue;
        }

        ChainId chain_id = delete_id_to_chain_id.at(delete_id);
        Chain& chain = chains.at(chain_id);
        const std::set<DeleteId>& chain_last_set = delete_sets[chain.back()];

        if (std::includes(current_set.begin(), current_set.end(), chain_last_set.begin(), chain_last_set.end())) {
          chain.emplace_back(set_id);
          for (DeleteId d : current_set) {
            delete_id_to_chain_id[d] = chain_id;
          }

          continues_some_chain = true;
          break;
        }
      }

      if (!continues_some_chain) {
        ChainId chain_id = chains.size();
        chains.emplace_back(std::vector<SetId>{set_id});

        for (DeleteId d : current_set) {
          delete_id_to_chain_id[d] = chain_id;
        }
      }
    }
  }

  return chains;
}

}  // namespace iceberg
