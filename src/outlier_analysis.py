import pandas as pd
import numpy as np
from export_stats import export_comprehensive_stats_to_csv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def analyze_outlier_impact():
    """Analyze how outliers affect scoring distribution"""
    
    # Get the comprehensive stats
    df = export_comprehensive_stats_to_csv()
    if df is None:
        return
    
    print("=== OUTLIER IMPACT ANALYSIS ===\n")
    
    # Find all stat columns (not scores)
    stat_columns = [col for col in df.columns if col.endswith('_Stats')]
    
    outlier_impact = []
    
    for col in stat_columns:
        if col in df.columns:
            values = df[col].dropna()
            if len(values) < 3:
                continue
                
            # Calculate distribution metrics
            mean_val = values.mean()
            std_val = values.std()
            min_val = values.min()
            max_val = values.max()
            median_val = values.median()
            
            # Find potential outliers (more than 2 std devs from mean)
            outliers_low = values[values < (mean_val - 2*std_val)]
            outliers_high = values[values > (mean_val + 2*std_val)]
            
            # Calculate range compression
            range_total = max_val - min_val
            range_without_outliers = values.quantile(0.9) - values.quantile(0.1)
            compression_ratio = range_without_outliers / range_total if range_total > 0 else 0
            
            # Calculate coefficient of variation
            cv = std_val / mean_val if mean_val != 0 else 0
            
            outlier_impact.append({
                'Category': col.replace('_Stats', ''),
                'Min': min_val,
                'Max': max_val,
                'Mean': mean_val,
                'Median': median_val,
                'Std_Dev': std_val,
                'CV': cv,
                'Low_Outliers': len(outliers_low),
                'High_Outliers': len(outliers_high),
                'Compression_Ratio': compression_ratio,
                'Range': range_total
            })
    
    # Convert to DataFrame and sort by compression ratio
    impact_df = pd.DataFrame(outlier_impact)
    impact_df = impact_df.sort_values('Compression_Ratio')
    
    print("TOP 6 CATEGORIES MOST AFFECTED BY OUTLIERS:")
    print("(Lower compression ratio = more outlier impact)")
    print("-" * 80)
    
    for i, row in impact_df.head(10).iterrows():
        print(f"{row['Category']:15} | Compression: {row['Compression_Ratio']:.3f} | "
              f"CV: {row['CV']:.3f} | Outliers: {row['Low_Outliers']}L/{row['High_Outliers']}H")
    
    return impact_df

def find_specific_team_issues(team_name=""):
    """Analyze specific team's performance vs ranking"""
    
    df = export_comprehensive_stats_to_csv()
    if df is None:
        return
    
    if not team_name:
        print("\nTEAM RANKINGS BY TOTAL SCORE:")
        print("-" * 50)
        for i, row in df.iterrows():
            print(f"Rank {int(row.get('Total_Score_Rank', 0)):2d}: {row['Team']:20} | "
                  f"Score: {row.get('Total_Score_Sum', 0):7.2f} | "
                  f"League Rank: {int(row.get('Rank', 0)):2d} | "
                  f"Variation: {row.get('Score_Variation', 0):+5.2f}")
        return
    
    # Find the specific team
    team_row = df[df['Team'].str.contains(team_name, case=False, na=False)]
    if team_row.empty:
        print(f"Team '{team_name}' not found")
        return
    
    team_data = team_row.iloc[0]
    
    print(f"\n=== ANALYSIS FOR {team_data['Team'].upper()} ===")
    print(f"Total Score Rank: {int(team_data.get('Total_Score_Rank', 0))}")
    print(f"League Rank: {int(team_data.get('Rank', 0))}")
    print(f"Score Variation: {team_data.get('Score_Variation', 0):+.2f}")
    print(f"Total Score: {team_data.get('Total_Score_Sum', 0):.2f}")
    
    # Find their best and worst categories
    score_columns = [col for col in df.columns if col.endswith('_Stats_Score')]
    
    team_scores = []
    for col in score_columns:
        if col in team_data:
            category = col.replace('_Stats_Score', '')
            score = team_data[col]
            
            # Find their rank in this category
            category_rank = (df[col] > score).sum() + 1
            
            team_scores.append({
                'Category': category,
                'Score': score,
                'Rank': category_rank
            })
    
    team_scores_df = pd.DataFrame(team_scores).sort_values('Score', ascending=False)
    
    print(f"\nTOP 10 CATEGORIES FOR {team_data['Team'].upper()}:")
    print("-" * 50)
    for i, row in team_scores_df.head(6).iterrows():
        print(f"{row['Category']:15} | Score: {row['Score']:6.2f} | Rank: {int(row['Rank']):2d}")
    
    print(f"\nBOTTOM 6 CATEGORIES FOR {team_data['Team'].upper()}:")
    print("-" * 50)
    for i, row in team_scores_df.tail(6).iterrows():
        print(f"{row['Category']:15} | Score: {row['Score']:6.2f} | Rank: {int(row['Rank']):2d}")

def compare_with_without_outliers():
    """Compare scoring with and without extreme outliers"""
    
    df = export_comprehensive_stats_to_csv()
    if df is None:
        return
    
    print("\n=== OUTLIER REMOVAL IMPACT ===")
    
    # Find stat columns
    stat_columns = [col for col in df.columns if col.endswith('_Stats')]
    
    # Create a copy for outlier-adjusted scoring
    df_adjusted = df.copy()
    
    for col in stat_columns:
        if col in df.columns:
            values = df[col].dropna()
            
            # Remove extreme outliers (beyond 2.5 standard deviations)
            mean_val = values.mean()
            std_val = values.std()
            
            # Cap outliers at 2.5 std devs
            lower_bound = mean_val - 2.5 * std_val
            upper_bound = mean_val + 2.5 * std_val
            
            df_adjusted[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
    
    print("This analysis would require re-running the scoring calculation...")
    print("Consider implementing outlier-resistant scoring methods:")
    print("1. Winsorization (cap extreme values)")
    print("2. Robust scaling (use median/IQR instead of mean/std)")
    print("3. Percentile-based scoring")

def main():
    """Run comprehensive outlier analysis"""
    
    print("Fantasy Baseball Outlier Impact Analysis")
    print("=" * 50)
    
    # 1. General outlier impact
    impact_df = analyze_outlier_impact()
    
    # 2. Show all team rankings
    find_specific_team_issues()
    
    # 3. Analyze specific team (you can change this)
    print("\n" + "="*50)
    team_to_analyze = input("Enter team name to analyze (or press Enter to skip): ").strip()
    if team_to_analyze:
        find_specific_team_issues(team_to_analyze)
    
    # 4. Outlier removal suggestions
    compare_with_without_outliers()
    
    print(f"\n=== RECOMMENDATIONS ===")
    print("1. Consider winsorizing extreme outliers (cap at 95th/5th percentiles)")
    print("2. Use robust scaling methods (median-based instead of min-max)")
    print("3. Weight categories by league importance/variance")
    print("4. Consider separate scoring for rate stats vs. counting stats")

if __name__ == '__main__':
    main()