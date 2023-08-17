import pandas as pd
import numpy as np

# Load the data
df = pd.read_csv('fantasy_rankings.csv')

# I prefer to define 'replacement' as starting roster replacement,
# not waiver wire replacement. To get this effect, I am adding 1 extra
# roster spot for RB and WR. For QB, I want the number to be about 1.6,
# which we can't simulate in the draft. The difference between the 12th
# best and 19th best QB is ~22 points, so I will bump QB by that much.
"""
df['Projected Fantasy Points'] = np.where(
    df['Position'] == 'QB',
        df['Projected Fantasy Points'] + 22,
    df['Projected Fantasy Points']
)
"""

# Constants and initial settings
num_teams = 12
total_picks = 96  # 12 teams x 8 players/team
roster_sizes = {'QB': 1, 'RB': 3, 'WR': 4, 'TE': 1}
players_list = df['Player'].tolist()
positions_list = df['Position'].tolist()
points_list = df['Projected Fantasy Points'].tolist()

# Function to simulate a single draft
def simulate_draft_optimized():
    shuffled_indices = np.random.permutation(len(players_list))
    rosters = [{'QB': 0, 'RB': 0, 'WR': 0, 'TE': 0, 'FLEX': 0} for _ in range(num_teams)]
    picked_players = [[] for _ in range(num_teams)]
    team_points = [0 for _ in range(num_teams)]
    for i in range(total_picks):
        team_idx = i % num_teams
        if i >= num_teams:  # Snake draft logic
            team_idx = num_teams - 1 - team_idx
        for index in shuffled_indices:
            position = positions_list[index]
            points = points_list[index]
            if rosters[team_idx][position] < roster_sizes[position] or \
              (position != 'QB' and rosters[team_idx]['FLEX'] < 1):
                if rosters[team_idx][position] < roster_sizes[position]:
                    rosters[team_idx][position] += 1
                else:
                    rosters[team_idx]['FLEX'] += 1
                team_points[team_idx] += points
                picked_players[team_idx].append(players_list[index])
                shuffled_indices = shuffled_indices[shuffled_indices != index]
                break
    return team_points, picked_players

# Monte Carlo Simulation
player_counts = {player: 0 for player in df['Player']}
num_simulations_optimized = 100000

for _ in range(num_simulations_optimized):
    team_points, picked_players = simulate_draft_optimized()
    best_team_idx = np.argmax(team_points)
    for player in picked_players[best_team_idx]:
        player_counts[player] += 1

# Identify the projected points of the replacement player for each position
replacement_points_dict = {
    'QB': df[df['Position'] == 'QB']['Projected Fantasy Points'].sort_values(ascending=False).iloc[11],
    'RB': df[df['Position'] == 'RB']['Projected Fantasy Points'].sort_values(ascending=False).iloc[35],
    'WR': df[df['Position'] == 'WR']['Projected Fantasy Points'].sort_values(ascending=False).iloc[47],
    'TE': df[df['Position'] == 'TE']['Projected Fantasy Points'].sort_values(ascending=False).iloc[11]
}

sorted_players_optimized = sorted(player_counts.keys(), key=lambda x: player_counts[x], reverse=True)
# Calculate the VORP for each player
df['Replacement Points'] = [replacement_points_dict.get(x) for x in df['Position']]
df['VORP'] = df['Projected Fantasy Points'] - df['Replacement Points']
df['Overall Rank Optimized'] = df['Player'].apply(lambda x: sorted_players_optimized.index(x) + 1)
df['Position Rank'] = df.groupby('Position')['Projected Fantasy Points'].rank(ascending=False)

# Output results
output_df = df[['Player', 'Position', 'Projected Fantasy Points', 'VORP', 'Overall Rank Optimized', 'Position Rank']]
output_df.columns = ['Player', 'Position', 'Projected Fantasy Points', 'VORP', 'Overall Rank', 'Position Rank']
output_df.to_csv('montecarlo_results.csv', index=False)

print("Simulation complete. Rankings saved to 'montecarlo_results.csv'.")
