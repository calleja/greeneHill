Plan + ready-to-run pandas/numpy recipe to turn the `leave_coop` column (lists of categories separated by `;`) into a correlation-style matrix showing "If respondent chose A, % also chose B" (i.e. P(B | A)). I include preprocessing, one-hot encoding, co‑occurrence, conditional probabilities, an optional phi (binary Pearson) correlation, and stability notes.

Steps (high level)
1. Clean & parse the column into consistent tokens (strip whitespace, drop empty tokens).
2. Build a respondent × category binary matrix (one-hot) where 1 = respondent selected that category.
3. Compute co-occurrence counts: for categories A,B the number of respondents who selected both.
4. Compute conditional probabilities P(B | A) = co_occurrence[A,B] / count(A).
5. Optionally compute symmetric measures: Jaccard, lift, or phi-correlation (binary Pearson).
6. Filter/smooth small counts and visualize (heatmap or table of percentages).

Code — copy/paste into your notebook (uses only pandas + numpy + matplotlib for display)
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# assume `survey2` exists and `leave_coop` is the column
s = survey2['leave_coop']

# 1) normalize and build one-hot matrix (handles NaN, trims whitespace, removes empty tokens)
clean = (
    s.fillna('')                              # replace NaN with empty
     .astype(str)
     .str.replace(r'\s*;\s*', ';', regex=True)  # normalize spaces around ';'
     .str.strip(';')                           # remove leading/trailing separators
)

# Use get_dummies on the normalized string. Empty string -> all zeros
ohe = clean.str.get_dummies(sep=';')

# If some cells had repeated tokens (unlikely), ensure binary (1/0)
ohe = ohe.clip(upper=1)

# 2) counts per category (number of respondents selecting that category)
counts = ohe.sum(axis=0)              # Series: index=category, value=count
n_respondents = len(ohe)

# 3) co-occurrence matrix (category x category): integer counts
co_occ = ohe.T.dot(ohe)               # DataFrame: rows A, cols B => count(A and B)

# 4) conditional probability matrix P(B | A) as percentages
# For each row A, divide by count(A) (avoid divide by zero)
cond_prob = co_occ.div(counts.replace(0, np.nan), axis=0)  # floats in [0,1]
cond_pct = (cond_prob * 100).round(1)                      # percentages, 1 decimal

# Example: P('price' | 'quality') is cond_pct.loc['quality', 'price']
print("Number respondents per category:\n", counts.sort_values(ascending=False).head(20))
print("\nCo-occurrence sample:\n", co_occ.iloc[:5, :5])
print("\nConditional probabilities (percent):\n", cond_pct.iloc[:10, :10])

# 5) Optional: phi (Pearson correlation for binary features)
phi = ohe.corr()   # DataFrame of Pearson correlations between binary indicator columns

# 6) Optional: Jaccard similarity (intersection / union)
jaccard = co_occ.copy()
# union(A,B) = count(A) + count(B) - co_occ[A,B]
for a in jaccard.index:
    for b in jaccard.columns:
        union = counts[a] + counts[b] - co_occ.at[a, b]
        jaccard.at[a, b] = co_occ.at[a, b] / union if union != 0 else np.nan

# 7) Presentation: reorder by frequency and show heatmap of P(B|A)
top_k = 12
top_cats = counts.sort_values(ascending=False).index[:top_k]
plt.figure(figsize=(10,8))
plt.imshow(cond_pct.loc[top_cats, top_cats], cmap='viridis', vmin=0, vmax=100)
plt.colorbar(label='P(B | A) %')
plt.xticks(range(top_k), top_cats, rotation=90)
plt.yticks(range(top_k), top_cats)
plt.title('Conditional %: P(B | A) (rows=A, cols=B)')
plt.show()
```

Notes & details
- Interpretation: row A, column B value = percent of respondents who chose A that also chose B. The diagonal will always be 100% (every A co-occurs with itself).
- If you want symmetric co-occurrence frequency normalized by total respondents use co_occ / n_respondents.
- The phi matrix (`ohe.corr()`) gives a Pearson correlation between binary columns; useful if you prefer a correlation coefficient (range -1..1) instead of conditional percentage.
- Jaccard is useful as a symmetric similarity (intersection / union).
- Avoid dividing by zero: categories with zero counts are set to NaN.
- Filtering/smoothing:
  - Exclude categories with very small counts (e.g., < 5 or configurable) before interpreting percentages — small denominators make percentages unstable.
  - Optional Laplace smoothing: cond_prob = (co_occ + alpha) / (counts + alpha * k) for small-sample smoothing (alpha small, k number of possible B categories).
- Duplicate tokens: `.str.get_dummies()` treats repeated tokens once; we clipped to 1 to be safe.
- If your categories have typos or inconsistent capitalization, normalize them first (lowercase, correct known variants).

Optional additions
- Preserve category order: you may want to set a stable ordering `categories = sorted(ohe.columns)` or order by counts.
- Save `cond_pct` to CSV: `cond_pct.to_csv('leave_coop_cond_pct.csv')`
- Compute and display top partner categories for each A:
```python
top_partners = {a: cond_pct.loc[a].drop(a).nlargest(5) for a in cond_pct.index}
# print top 5 partners for the most frequent categories
for a in counts.sort_values(ascending=False).index[:10]:
    print(a, '\n', top_partners[a], '\n')
```

Performance
- For a typical survey (hundreds–thousands rows, tens–low hundreds categories) the above is fast.
- If respondents >>100k or categories very many, consider sparse structures or aggregating categories first.

If you want, I can:
- Suggest code to canonicalize category labels (lowercasing, mapping synonyms).
- Add Laplace smoothing example.
- Produce a nicer matplotlib/seaborn heatmap (annotated) and a CSV export of `cond_pct`. Which would you like next?