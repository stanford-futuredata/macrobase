#ifndef _COMPARE_BY_H_
#define _COMPARE_BY_H_

#include <stdint.h>
#include <limits>
#include <string>
#include "util.h"

using std::numeric_limits;

typedef double (*macrodiff_compare_by_func)(const uint32_t, const uint32_t,
                                            const uint32_t, const uint32_t);

/**
 * P(exposure| outlier ) / P(exposure | ~outlier)
 */
inline double prevalence_ratio(const uint32_t matched_outlier,
                               const uint32_t matched_total,
                               const uint32_t outlier_count,
                               const uint32_t total_count) {
  if (outlier_count == 0 || matched_outlier == 0) {
    return 0;
  }

  const uint32_t inlier_count = total_count - outlier_count;
  uint32_t matched_inlier = matched_total - matched_outlier;

  if (matched_inlier == 0) {
    matched_inlier += 1;  // increment by 1 to avoid divide-by-zero error
  }

  return ((double)matched_outlier / outlier_count) /
         ((double)matched_inlier / inlier_count);
}

/**
 * P(outlier | exposure) / P(outlier | ~exposure)
 * Used in epidemiology
 */
inline double risk_ratio(const uint32_t matched_outlier,
                         const uint32_t matched_total,
                         const uint32_t outlier_count,
                         const uint32_t total_count) {
  const double unmatched_outlier = outlier_count - matched_outlier;
  const double unmatched_total = total_count - matched_total;

  if (matched_total == 0 || unmatched_total == 0) {
    return 0;
  }
  // all outliers had this pattern
  if (unmatched_outlier == 0) {
    return numeric_limits<double>::max();
  }

  return (matched_outlier / matched_total) /
         (unmatched_outlier / unmatched_total);
}

/**
 * P(outlier | exposure) / P(outlier)
 * Exponential pairwise mutual information
 * Doesn't have NaN / Infty errors in edge cases as much as risk ratio does
 */
inline double pmi_ratio(const uint32_t matched_outlier,
                        const uint32_t matched_total,
                        const uint32_t outlier_count,
                        const uint32_t total_count) {
  return (matched_outlier / (double)matched_total) /
         (outlier_count / (double)total_count);
}

// TODO: code-generation maybe?
macrodiff_compare_by_func get_compare_by_func(const std::string fn_name) {
  if (equals_ignorecase(fn_name, "prevalence_ratio")) {
    return prevalence_ratio;
  } else if (equals_ignorecase(fn_name, "pmi_ratio")) {
    return pmi_ratio;
  } else {
    // default, TODO: throw exception if fn_name isn't valid
    // TODO: risk_ratio
    return pmi_ratio;
  }
}
#endif
