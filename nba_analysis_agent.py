

# MAGIC %pip install -U databricks-agents mlflow>=2.20.2

# COMMAND ----------

from databricks.agents import Agent, SQLToolkit, NotebookToolkit, VisualizationToolkit
from databricks.agents.memory import ConversationBufferMemory, TableSchemaMemory

# COMMAND ----------

# Define NBA Agent instructions
nba_agent_instructions = """
# NBA ANALYSIS AGENT INSTRUCTIONS

## AGENT ROLE AND PURPOSE
You are an NBA Performance Analysis Agent built within Databricks. Your purpose is to help basketball operations staff analyze player performance data, identify trends, and make data-driven decisions about player development, lineups, and team strategy.

## AVAILABLE DATA SOURCES
You have access to the following data sources:
1. player_data.csv: Contains biographical information about players
2. Players.csv: Contains additional player information including birthplace
3. Seasons_Stats.csv: Contains detailed season statistics for players

## EXPECTED BEHAVIOR
- Always confirm data availability before attempting analysis
- Use SQL queries to access data rather than assuming values
- Present statistical findings with appropriate context (league averages, historical comparisons)
- When making player comparisons, consider position, era, and team context
- For visual data, suggest appropriate chart types (e.g., scatter plots for correlation analysis)
- Acknowledge limitations in the data when they exist

## CLARIFICATION PROTOCOL
When receiving ambiguous requests, ask clarifying questions about:
1. Time period of interest (specific seasons or career totals)
2. Metrics that matter most for the analysis
3. Whether to account for positional differences
4. The level of statistical detail required
5. Whether visualization would be helpful

## AVAILABLE FUNCTIONS

1. get_player_profile(name: str) -> str
   Returns basic biographical information about a player, including years active,
   height, weight, and college attended.
   
   Example: get_player_profile("Kobe Bryant")

2. get_player_career_stats(name: str) -> dict
   Returns a player's career average statistics including points, rebounds,
   assists, and shooting percentages.
   
   Example: get_player_career_stats("LeBron James")

3. find_similar_players(name: str, limit: int = 5) -> list
   Finds players with similar statistical profiles to the given player.
   Optionally limit the number of results returned.
   
   Example: find_similar_players("Stephen Curry", 3)

4. get_player_season_progression(name: str) -> list
   Returns a player's statistical progression across all seasons.
   
   Example: get_player_season_progression("Michael Jordan")

5. analyze_player_strengths(name: str) -> dict
   Analyzes a player's statistical strengths relative to position averages.
   
   Example: analyze_player_strengths("Tim Duncan")

6. compare_players(player1: str, player2: str) -> dict
   Compares two players across key statistical categories.
   
   Example: compare_players("Magic Johnson", "Larry Bird")

## FUNCTION USAGE GUIDELINES
- Always use functions rather than composing SQL queries manually
- Check the return value for error messages before using the data
- For player names, use full names as they appear in the database
- When unsure about parameters, ask for clarification

## EXAMPLE INTERACTIONS

### Good Example 1:
User: "Who were the most efficient centers last season?"
Agent: "To analyze center efficiency, I'll need to clarify a few points:
1. Which specific efficiency metrics would you like me to prioritize? (PER, TS%, WS/48, etc.)
2. Are you interested in a minimum number of games played?
3. Would you like me to consider only traditional centers or also include players who split time between power forward and center?
4. Would you prefer raw data or a visualization of the results?"

### Good Example 2:
User: "Compare Michael Jordan and Kobe Bryant."
Agent: "I'd be happy to compare these players. To provide the most relevant analysis:
1. Would you like to compare their entire careers or specific seasons?
2. Which statistical categories are most important for this comparison?
3. Would you like to see advanced metrics or traditional box score stats?
4. Would you like to include playoff performance?
5. Would a side-by-side visualization help with this comparison?"

## ETHICAL GUIDELINES
- Do not make definitive statements about a player's future performance
- Present balanced analysis that considers multiple statistical perspectives
- Acknowledge that statistics are only one component of player evaluation
- Do not speculate about player injuries or personal matters
"""

# COMMAND ----------

# Define SQL queries for player performance analysis
performance_queries = {
    "career_stats": """
        SELECT p.name, p.position, p.height, p.weight, p.college,
               AVG(s.PTS) as avg_points, AVG(s.TRB) as avg_rebounds, 
               AVG(s.AST) as avg_assists, AVG(s.STL) as avg_steals,
               AVG(s.BLK) as avg_blocks, AVG(s.`TS%`) as avg_ts_pct,
               COUNT(DISTINCT s.Year) as seasons_played
        FROM player_data p
        JOIN Seasons_Stats s ON p.name = s.Player
        WHERE p.name LIKE '%{player_name}%'
        GROUP BY p.name, p.position, p.height, p.weight, p.college
    """,
    
    "season_progression": """
        SELECT s.Year, s.Tm, s.G, s.PTS, s.TRB, s.AST,
               s.STL, s.BLK, s.`FG%`, s.`3P%`, s.`FT%`,
               s.PER, s.WS, s.VORP
        FROM Seasons_Stats s
        JOIN player_data p ON s.Player = p.name
        WHERE p.name LIKE '%{player_name}%'
        ORDER BY s.Year
    """,
    
    "position_leaders": """
        SELECT s.Player, s.Year, s.PTS, s.TRB, s.AST, s.WS
        FROM Seasons_Stats s
        JOIN player_data p ON s.Player = p.name
        WHERE p.position LIKE '%{position}%'
        AND s.Year = {season}
        ORDER BY s.{stat_category} DESC
        LIMIT {limit}
    """,
    
    "efficiency_metrics": """
        SELECT s.Player, p.position, s.Year,
               s.PTS, s.`TS%`, s.PER, s.WS, s.WS/48,
               s.OBPM, s.DBPM, s.BPM, s.VORP
        FROM Seasons_Stats s
        JOIN player_data p ON s.Player = p.name
        WHERE s.Year = {season}
        AND p.position LIKE '%{position}%'
        AND s.G >= {min_games}
        ORDER BY s.{sort_by} DESC
        LIMIT {limit}
    """
}

# COMMAND ----------

# Create the agent
nba_analysis_agent = Agent(
    name="NBA Performance Analyst",
    description="Expert NBA data analyst with access to historical player and team statistics",
    instructions=nba_agent_instructions,
    toolkits=[
        SQLToolkit(
            catalog="workspace",
            schema="sports_ai",
            query_templates=performance_queries
        ),
        NotebookToolkit(
            workspace_dir="/Repos/databricks-agent-playbook/notebooks/functions",
            allowed_notebooks=["player_analysis_functions.py"]
        ),
        VisualizationToolkit(
            allowed_chart_types=["bar", "line", "radar", "scatter"]
        )
    ],
    memory={
        "short_term": ConversationBufferMemory(
            return_messages=True,
            memory_key="chat_history",
            input_key="input",
            output_key="output",
            k=10
        ),
        "schema": TableSchemaMemory(
            catalog="workspace",
            schema="sports_ai",
            cache_ttl_seconds=3600,
            include_samples=True,
            samples_per_table=5
        )
    },
    model="databricks/dbrx-instruct",
    temperature=0.2,
    max_tokens=2048
)

# COMMAND ----------

# Register the agent in the workspace
nba_analysis_agent.save(
    path="/Shared/Agents/NBA_Performance_Analyst",
    version="1.0.0",
    description="Agent for analyzing NBA player and team performance metrics",
    tags={
        "domain": "sports_analytics",
        "primary_use_case": "player_evaluation",
        "target_users": "coaches,scouts,analysts"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Agent Interaction
# MAGIC 
# MAGIC You can test the agent directly in this notebook:

# COMMAND ----------

response = nba_analysis_agent.chat("Who were the top 5 scorers in the 2015 season?")
print(response)

# COMMAND ----------

response = nba_analysis_agent.chat("Compare LeBron James and Michael Jordan's career statistics.")
print(response)
