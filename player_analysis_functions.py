# Set up the catalog and schema
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA sports_ai")

# COMMAND ----------

def get_player_profile(name: str) -> str:
    """
    Returns a player's career timeline and basic bio info.
    
    Args:
        name: The player's name
        
    Returns:
        A string with the player's profile information
    """
    df = spark.table("player_data")
    result = df.filter(df.name == name).limit(1).collect()
    
    if not result:
        return f"No profile found for {name}."
    
    row = result[0]
    return (
        f"{row['name']} played from {row['year_start']} to {row['year_end']}, "
        f"stood {row['height']}, weighed {row['weight']} lbs, "
        f"and went to {row['college']}."
    )

# COMMAND ----------

def get_player_career_stats(name: str) -> dict:
    """
    Returns a player's career average statistics.
    
    Args:
        name: The player's name
        
    Returns:
        Dictionary with career averages for key statistics
    """
    # Query the seasons stats table
    stats_df = spark.sql(f"""
        SELECT 
            Player,
            COUNT(DISTINCT Year) as seasons,
            AVG(PTS) as ppg,
            AVG(TRB) as rpg,
            AVG(AST) as apg,
            AVG(`FG%`) as fg_pct,
            AVG(`3P%`) as fg3_pct,
            AVG(`FT%`) as ft_pct,
            AVG(WS) as win_shares
        FROM Seasons_Stats
        WHERE Player = '{name}'
        GROUP BY Player
    """)
    
    result = stats_df.collect()
    
    if not result:
        return {"error": f"No stats found for {name}."}
    
    row = result[0]
    return {
        "player": row["Player"],
        "seasons": int(row["seasons"]),
        "ppg": round(float(row["ppg"]), 1),
        "rpg": round(float(row["rpg"]), 1),
        "apg": round(float(row["apg"]), 1),
        "fg_pct": round(float(row["fg_pct"]) * 100, 1),
        "fg3_pct": round(float(row["fg3_pct"]) * 100, 1) if row["fg3_pct"] is not None else None,
        "ft_pct": round(float(row["ft_pct"]) * 100, 1),
        "win_shares": round(float(row["win_shares"]), 1)
    }

# COMMAND ----------

def find_similar_players(name: str, limit: int = 5) -> list:
    """
    Finds players with similar statistical profiles to the given player.
    
    Args:
        name: The reference player's name
        limit: Maximum number of similar players to return
        
    Returns:
        List of dictionaries with similar players and similarity scores
    """
    # First, get the target player's career averages
    target_stats = spark.sql(f"""
        SELECT 
            Player,
            AVG(PTS) as pts,
            AVG(TRB) as trb,
            AVG(AST) as ast,
            AVG(STL) as stl,
            AVG(BLK) as blk,
            AVG(`TS%`) as ts_pct,
            AVG(PER) as per
        FROM Seasons_Stats
        WHERE Player = '{name}'
        GROUP BY Player
    """).collect()
    
    if not target_stats:
        return [{"error": f"No stats found for {name}."}]
    
    # Get the target player's position
    position_data = spark.table("player_data")
    position_result = position_data.filter(position_data.name == name).select("position").collect()
    
    if not position_result:
        position_filter = ""
    else:
        position = position_result[0]["position"]
        position_filter = f"AND p.position LIKE '%{position}%'"
    
    # Now find similar players using a similarity score calculation
    similar_players = spark.sql(f"""
        WITH target_stats AS (
            SELECT 
                '{name}' as player,
                {target_stats[0]['pts']} as pts,
                {target_stats[0]['trb']} as trb,
                {target_stats[0]['ast']} as ast,
                {target_stats[0]['stl']} as stl,
                {target_stats[0]['blk']} as blk,
                {target_stats[0]['ts_pct']} as ts_pct,
                {target_stats[0]['per']} as per
        ),
        player_stats AS (
            SELECT 
                s.Player,
                AVG(s.PTS) as pts,
                AVG(s.TRB) as trb,
                AVG(s.AST) as ast,
                AVG(s.STL) as stl,
                AVG(s.BLK) as blk,
                AVG(s.`TS%`) as ts_pct,
                AVG(s.PER) as per
            FROM Seasons_Stats s
            JOIN player_data p ON s.Player = p.name
            WHERE s.Player != '{name}' {position_filter}
            GROUP BY s.Player
            HAVING COUNT(DISTINCT s.Year) >= 3
        )
        SELECT 
            p.Player,
            p.pts, p.trb, p.ast,
            POWER(p.pts - t.pts, 2) + 
            POWER(p.trb - t.trb, 2) * 1.5 + 
            POWER(p.ast - t.ast, 2) * 1.5 + 
            POWER(p.stl - t.stl, 2) * 2 + 
            POWER(p.blk - t.blk, 2) * 2 + 
            POWER((p.ts_pct - t.ts_pct) * 100, 2) * 0.5 + 
            POWER(p.per - t.per, 2) * 3 as similarity_score
        FROM player_stats p, target_stats t
        ORDER BY similarity_score ASC
        LIMIT {limit}
    """).collect()
    
    return [
        {
            "player": row["Player"],
            "similarity_score": round(float(row["similarity_score"]), 2),
            "ppg": round(float(row["pts"]), 1),
            "rpg": round(float(row["trb"]), 1),
            "apg": round(float(row["ast"]), 1)
        }
        for row in similar_players
    ]

# COMMAND ----------

def get_player_season_progression(name: str) -> list:
    """
    Returns a player's statistical progression across seasons.
    
    Args:
        name: The player's name
        
    Returns:
        List of dictionaries with stats for each season
    """
    season_stats = spark.sql(f"""
        SELECT 
            Year,
            Tm as team,
            G as games,
            PTS as ppg,
            TRB as rpg,
            AST as apg,
            STL as spg,
            BLK as bpg,
            `FG%` as fg_pct,
            `3P%` as fg3_pct,
            `FT%` as ft_pct,
            PER,
            WS as win_shares
        FROM Seasons_Stats
        WHERE Player = '{name}'
        ORDER BY Year
    """).collect()
    
    if not season_stats:
        return [{"error": f"No season stats found for {name}."}]
    
    return [
        {
            "season": int(row["Year"]),
            "team": row["team"],
            "games": int(row["games"]),
            "ppg": round(float(row["ppg"]), 1),
            "rpg": round(float(row["rpg"]), 1),
            "apg": round(float(row["apg"]), 1),
            "spg": round(float(row["spg"]), 1) if row["spg"] is not None else None,
            "bpg": round(float(row["bpg"]), 1) if row["bpg"] is not None else None,
            "fg_pct": round(float(row["fg_pct"]) * 100, 1) if row["fg_pct"] is not None else None,
            "fg3_pct": round(float(row["fg3_pct"]) * 100, 1) if row["fg3_pct"] is not None else None,
            "ft_pct": round(float(row["ft_pct"]) * 100, 1) if row["ft_pct"] is not None else None,
            "per": round(float(row["PER"]), 1) if row["PER"] is not None else None,
            "win_shares": round(float(row["win_shares"]), 1) if row["win_shares"] is not None else None
        }
        for row in season_stats
    ]

# COMMAND ----------

def analyze_player_strengths(name: str) -> dict:
    """
    Analyzes a player's statistical strengths relative to position averages.
    
    Args:
        name: The player's name
        
    Returns:
        Dictionary with strength analysis in key statistical categories
    """
    # Get player position
    position_data = spark.table("player_data")
    position_result = position_data.filter(position_data.name == name).select("position").collect()
    
    if not position_result:
        return {"error": f"No position data found for {name}."}
    
    position = position_result[0]["position"]
    
    # Calculate position averages
    position_avg = spark.sql(f"""
        WITH position_players AS (
            SELECT s.Player, p.position
            FROM Seasons_Stats s
            JOIN player_data p ON s.Player = p.name
            WHERE p.position LIKE '%{position}%'
            GROUP BY s.Player, p.position
        )
        SELECT 
            AVG(s.PTS) as avg_pts,
            AVG(s.TRB) as avg_trb,
            AVG(s.AST) as avg_ast,
            AVG(s.STL) as avg_stl,
            AVG(s.BLK) as avg_blk,
            AVG(s.`TS%`) as avg_ts_pct,
            AVG(s.PER) as avg_per
        FROM Seasons_Stats s
        JOIN position_players p ON s.Player = p.Player
    """).collect()[0]
    
    # Get player stats
    player_stats = spark.sql(f"""
        SELECT 
            AVG(PTS) as pts,
            AVG(TRB) as trb,
            AVG(AST) as ast,
            AVG(STL) as stl,
            AVG(BLK) as blk,
            AVG(`TS%`) as ts_pct,
            AVG(PER) as per
        FROM Seasons_Stats
        WHERE Player = '{name}'
        GROUP BY Player
    """).collect()
    
    if not player_stats:
        return {"error": f"No stats found for {name}."}
    
    player_stats = player_stats[0]
    
    # Calculate percentages above/below average
    pts_pct = (player_stats["pts"] / position_avg["avg_pts"] - 1) * 100
    trb_pct = (player_stats["trb"] / position_avg["avg_trb"] - 1) * 100
    ast_pct = (player_stats["ast"] / position_avg["avg_ast"] - 1) * 100
    stl_pct = (player_stats["stl"] / position_avg["avg_stl"] - 1) * 100 if player_stats["stl"] is not None and position_avg["avg_stl"] is not None else None
    blk_pct = (player_stats["blk"] / position_avg["avg_blk"] - 1) * 100 if player_stats["blk"] is not None and position_avg["avg_blk"] is not None else None
    ts_pct = (player_stats["ts_pct"] / position_avg["avg_ts_pct"] - 1) * 100 if player_stats["ts_pct"] is not None and position_avg["avg_ts_pct"] is not None else None
    per_pct = (player_stats["per"] / position_avg["avg_per"] - 1) * 100 if player_stats["per"] is not None and position_avg["avg_per"] is not None else None
    
    # Determine strengths and weaknesses
    strengths = []
    weaknesses = []
    
    categories = [
        {"name": "scoring", "value": pts_pct},
        {"name": "rebounding", "value": trb_pct},
        {"name": "playmaking", "value": ast_pct},
        {"name": "defense", "value": (stl_pct + blk_pct) / 2 if stl_pct is not None and blk_pct is not None else None},
        {"name": "efficiency", "value": ts_pct},
        {"name": "overall impact", "value": per_pct}
    ]
    
    for category in categories:
        if category["value"] is not None:
            if category["value"] > 15:
                strengths.append(category["name"])
            elif category["value"] < -15:
                weaknesses.append(category["name"])
    
    return {
        "player": name,
        "position": position,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "stats_vs_position": {
            "ppg": {
                "player": round(float(player_stats["pts"]), 1),
                "position_avg": round(float(position_avg["avg_pts"]), 1),
                "pct_difference": round(pts_pct, 1)
            },
            "rpg": {
                "player": round(float(player_stats["trb"]), 1),
                "position_avg": round(float(position_avg["avg_trb"]), 1),
                "pct_difference": round(trb_pct, 1)
            },
            "apg": {
                "player": round(float(player_stats["ast"]), 1),
                "position_avg": round(float(position_avg["avg_ast"]), 1),
                "pct_difference": round(ast_pct, 1)
            },
            "per": {
                "player": round(float(player_stats["per"]), 1) if player_stats["per"] is not None else None,
                "position_avg": round(float(position_avg["avg_per"]), 1) if position_avg["avg_per"] is not None else None,
                "pct_difference": round(per_pct, 1) if per_pct is not None else None
            }
        }
    }

# COMMAND ----------

def compare_players(player1: str, player2: str) -> dict:
    """
    Compares two players across key statistical categories.
    
    Args:
        player1: First player's name
        player2: Second player's name
        
    Returns:
        Dictionary with comparative statistics
    """
    # Get career stats for both players
    comparison_stats = spark.sql(f"""
        WITH player1_stats AS (
            SELECT 
                '{player1}' as player,
                AVG(PTS) as pts,
                AVG(TRB) as trb,
                AVG(AST) as ast,
                AVG(STL) as stl,
                AVG(BLK) as blk,
                AVG(`TS%`) as ts_pct,
                AVG(PER) as per,
                AVG(WS) as ws,
                COUNT(DISTINCT Year) as seasons
            FROM Seasons_Stats
            WHERE Player = '{player1}'
            GROUP BY Player
        ),
        player2_stats AS (
            SELECT 
                '{player2}' as player,
                AVG(PTS) as pts,
                AVG(TRB) as trb,
                AVG(AST) as ast,
                AVG(STL) as stl,
                AVG(BLK) as blk,
                AVG(`TS%`) as ts_pct,
                AVG(PER) as per,
                AVG(WS) as ws,
                COUNT(DISTINCT Year) as seasons
            FROM Seasons_Stats
            WHERE Player = '{player2}'
            GROUP BY Player
        )
        SELECT 
            p1.player as player1,
            p2.player as player2,
            p1.pts as p1_pts,
            p2.pts as p2_pts,
            p1.trb as p1_trb,
            p2.trb as p2_trb,
            p1.ast as p1_ast,
            p2.ast as p2_ast,
            p1.stl as p1_stl,
            p2.stl as p2_stl,
            p1.blk as p1_blk,
            p2.blk as p2_blk,
            p1.ts_pct as p1_ts_pct,
            p2.ts_pct as p2_ts_pct,
            p1.per as p1_per,
            p2.per as p2_per,
            p1.ws as p1_ws,
            p2.ws as p2_ws,
            p1.seasons as p1_seasons,
            p2.seasons as p2_seasons
        FROM player1_stats p1, player2_stats p2
    """).collect()
    
    if not comparison_stats:
        return {"error": f"Could not find stats for both {player1} and {player2}."}
    
    row = comparison_stats[0]
    
    # Get position data
    positions = spark.sql(f"""
        SELECT 
            name, position
        FROM player_data
        WHERE name IN ('{player1}', '{player2}')
    """).collect()
    
    position_map = {p["name"]: p["position"] for p in positions}
    
    # Format the comparison
    return {
        "players": {
            "player1": {
                "name": player1,
                "position": position_map.get(player1, "Unknown")
            },
            "player2": {
                "name": player2,
                "position": position_map.get(player2, "Unknown")
            }
        },
        "career_stats": {
            "ppg": {
                "player1": round(float(row["p1_pts"]), 1),
                "player2": round(float(row["p2_pts"]), 1),
                "difference": round(float(row["p1_pts"] - row["p2_pts"]), 1)
            },
            "rpg": {
                "player1": round(float(row["p1_trb"]), 1),
                "player2": round(float(row["p2_trb"]), 1),
                "difference": round(float(row["p1_trb"] - row["p2_trb"]), 1)
            },
            "apg": {
                "player1": round(float(row["p1_ast"]), 1),
                "player2": round(float(row["p2_ast"]), 1),
                "difference": round(float(row["p1_ast"] - row["p2_ast"]), 1)
            },
            "spg": {
                "player1": round(float(row["p1_stl"]), 1) if row["p1_stl"] is not None else None,
                "player2": round(float(row["p2_stl"]), 1) if row["p2_stl"] is not None else None,
                "difference": round(float(row["p1_stl"] - row["p2_stl"]), 1) if row["p1_stl"] is not None and row["p2_stl"] is not None else None
            },
            "bpg": {
                "player1": round(float(row["p1_blk"]), 1) if row["p1_blk"] is not None else None,
                "player2": round(float(row["p2_blk"]), 1) if row["p2_blk"] is not None else None,
                "difference": round(float(row["p1_blk"] - row["p2_blk"]), 1) if row["p1_blk"] is not None and row["p2_blk"] is not None else None
            },
            "ts_pct": {
                "player1": round(float(row["p1_ts_pct"]) * 100, 1) if row["p1_ts_pct"] is not None else None,
                "player2": round(float(row["p2_ts_pct"]) * 100, 1) if row["p2_ts_pct"] is not None else None,
                "difference": round(float(row["p1_ts_pct"] - row["p2_ts_pct"]) * 100, 1) if row["p1_ts_pct"] is not None and row["p2_ts_pct"] is not None else None
            },
            "per": {
                "player1": round(float(row["p1_per"]), 1) if row["p1_per"] is not None else None,
                "player2": round(float(row["p2_per"]), 1) if row["p2_per"] is not None else None,
                "difference": round(float(row["p1_per"] - row["p2_per"]), 1) if row["p1_per"] is not None and row["p2_per"] is not None else None
            },
            "win_shares": {
                "player1": round(float(row["p1_ws"]), 1) if row["p1_ws"] is not None else None,
                "player2": round(float(row["p2_ws"]), 1) if row["p2_ws"] is not None else None,
                "difference": round(float(row["p1_ws"] - row["p2_ws"]), 1) if row["p1_ws"] is not None and row["p2_ws"] is not None else None
            },
            "seasons_played": {
                "player1": int(row["p1_seasons"]),
                "player2": int(row["p2_seasons"]),
                "difference": int(row["p1_seasons"] - row["p2_seasons"])
            }
        }
    }
