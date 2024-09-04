import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import sqlite3
import seaborn as sns


def show_conf_tab():
    """ This function gets list of confederations and returns a nicely formatted table plot"""   


    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties
    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')

    #build parameter into sqlstring


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""
        SELECT  confederation_code,
                confederation_name
        FROM confederations"""

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    #rename cols
    df.columns = ['Confederation Code','Confederation Name']



    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(10, 2))  # Adjust the size as needed

    # Hide the axes
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)
    ax.set_frame_on(False)

    # Create the table
    table = ax.table(cellText=df.values,  # Data for the table
                    colLabels=df.columns,  # Column headers
                    cellLoc='center',  # Center-align cells
                    loc='center',  # Center the table
                    bbox=[0, 0, 1, 1])  # Bounding box for the table
    
    
    # Bold the column headers

    # Bold the column headers (first row)
    header_font = FontProperties(weight='bold')  # Set font properties for bold headers
    table[(0, 0)].set_text_props(fontweight='bold', fontsize=12)
    table[(0, 1)].set_text_props(fontweight='bold', fontsize=12)

    # Set font size for table cells
    table.auto_set_font_size(True)
    #table.set_fontsize(12)

    # Set column width
    table.auto_set_column_width([0, 1])

    # Display the plot with the table
    plt.title('Confederations', fontsize=14)
    #plt.show()

        
    return plt

############################################################ Query DB Functions #########################################


def qry_ref_stats(p_ccodelist=None)  -> pd.DataFrame:
    """ This function retrieves some summery stats on referee participation in World Cup and returns a df"""   
    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    cclist = p_ccodelist
    #def qry_ref_stats(p_ccode=None):

    extra=''
    newlist=''
    
        #build parameter into sqlstring
    if cclist == None:
        x=1

    else:
        for i, a in enumerate(cclist):
            if i == 0:  # Check if it is the first element
                newlist += f"'{a}'"  # Do not add a comma for the first element
            else:
                newlist += f",'{a}'"  # Add a comma for subsequent elements
      
    
    extra = f"and confederation_code not in ({newlist})" 
   
    myqry = f"""
    with ref_summary as(
    select ra.tournament_id
    , t.year
    , ra.confederation_code
    ,ra.referee_id
    /*
    , count(distinct referee_id) numref */
    ,sum(count(*))        over(partition by ra.tournament_id, ra.confederation_code)     ref_matches_conf
    ,sum(count(distinct ra.referee_id))        over(partition by ra.tournament_id, ra.confederation_code)     num_refs_conf
    ,sum(count(*))        over(partition by ra.tournament_id)     ref_matches_tourn
    ,sum(count(distinct ra.referee_id))        over(partition by ra.tournament_id)     num_refs_tourn

    from referee_appearances ra
    left join  tournaments_men t
        on ra.tournament_id = t.tournament_id
    where 1=1  
    {extra}

    group by
    ra.tournament_id
    , t.year
    ,ra.confederation_code
    ,ra.referee_id
    )
    select  
    tournament_id
    ,year
    ,confederation_code
    ,avg(num_refs_conf) tot_refs_conf
    ,avg(num_refs_tourn) tot_refs_tourn
    ,avg (cast(num_refs_conf as decimal))/ avg(cast(num_refs_tourn as decimal))*100 pc_total_refs_tourn
    ,avg(ref_matches_conf) tot_refmatch_conf
    ,avg(ref_matches_tourn) tot_refmatch_tourn
    ,avg (cast(ref_matches_conf as decimal))/ avg(cast(ref_matches_tourn as decimal))*100 pc_game_reffed


    from ref_summary
    where 1=1
    {extra}
    group by 
    tournament_id
    ,year
    ,confederation_code
    """
 
    #print(f"{extra}")
    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    return df

def qry_conf_book(p_ccode=None)  -> pd.DataFrame:
    
    """ This function retrieves some summary stats for game/refs and cards by tourn and returnsdf"""   

    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    ccode = p_ccode
    #def qry_ref_stats(p_ccode=None):

    #build parameter into sqlstring
    if ccode == None:
        extra=''
    else:
        extra = f"where something != '{ccode}'" 


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""
  SELECT 
       [tournament_year]
       ,   [ref_confed]
       ,[team_booked_confed]

      ,sum([total_yellow] )  yell
      ,sum([total_red]) red
      ,sum([total_cards]) tot_cards
      ,sum([second_yellow_card])  yell2
      ,sum([sending_off]) soff
      ,avg([player_age])  av_age
      
  FROM [bookings_dn]
  group by [tournament_year]
      ,[ref_confed]
      ,[team_booked_confed]


               """

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    
    return df


def qry_tourn_counts(p_ccode=None)  -> pd.DataFrame:

    
    """ This function retrieves some summary stats for game/refs and cards by tourn and returnsdf"""   

    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    ccode = p_ccode
    #def qry_ref_stats(p_ccode=None):

    #build parameter into sqlstring
    if ccode == None:
        extra=''
    else:
        extra = f"where something != '{ccode}'" 


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""
                    select t.tournament_id
            ,t.year
            ,t.count_teams
            , count(distinct(m.match_id)) count_matches

            , (select count(distinct ra.referee_id) from referee_appearances ra
            where t.tournament_id = ra.tournament_id) count_refs
            , count(distinct(m.match_id)) / (select cast(count(distinct ra.referee_id) as float)
                                                from referee_appearances ra
                                                where t.tournament_id = ra.tournament_id)*6 ref_game_ratio

            ,	(select sum(cast(b.total_yellow as float))/15	from bookings_dn b
    
                where t.tournament_id = b.tournament_id) num_yellow_rat
            ,	(select sum(b.total_red)	from bookings_dn b
                where t.tournament_id = b.tournament_id) num_red
            ,	(select sum(cast(b.yellow_card as float))	from bookings_dn b
                where t.tournament_id = b.tournament_id) num_yellow
            /*
            into tournament_stats
            */

            FROM matches m
            left join tournaments_men t
            on m.tournament_id = t.tournament_id
            left join bookings_dn b
            on b.tournament_id = t.tournament_id


            group by 
            t.tournament_id
            ,t.year
            ,t.count_teams
            order by t.year
                """

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    
    return df



def qry_book_dn_conf(p_ccode=None)  -> pd.DataFrame:
    """ This function retrieves confed data from  bookings denorm in World Cup and returns a df"""   

    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    ccode = p_ccode
    #def qry_ref_stats(p_ccode=None):

    #build parameter into sqlstring
    if ccode == None:
        extra=''
    else:
        extra = f"where something != '{ccode}'" 


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""

SELECT bk_key_id,
       booking_id,
       tournament_host_country,
       tournament_year,
      match_date,
     match_name,
      host_country_team_booking,
       host_country,
       ref_team_same_confed,
       ref_country_name,
       ref_confed,
       host_country_conf,
       team_booked_confed,
       team_booked,
       home_away_booked,
       total_cards,
       total_yellow,
       total_red,
       second_yellow_card,
       sending_off,
       tournament_name,
       stadium_country_name,
       stadium_capacity,
       match_period,
       in_stoppage
  FROM bookings_dn;


    """

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    
    return df

def qry_cardav(p_ccode=None)  -> pd.DataFrame:
    
    """ This function retrieves some summery stats on card avgs and returns a df"""   

    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    ccode = p_ccode
    #def qry_ref_stats(p_ccode=None):

    #build parameter into sqlstring
    if ccode == None:
        extra=''
    else:
        extra = f"where something != '{ccode}'" 


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""
 select ts.tournament_id
    ,ts.count_teams
    ,ts.count_matches
    ,sum(b.total_yellow) yellows

    ,sum(cast(b.total_yellow as float))/cast(ts.count_matches as float) av_yell_pg

    ,sum(cast(b.total_red as float))/cast(ts.count_matches as float) av_red_pg


    from tournament_stats ts
    left join bookings_dn b
    on b.tournament_id = ts.tournament_id
    group by
    ts.tournament_id
    ,ts.count_teams
    ,ts.count_matches
    order by
    ts.tournament_id

    """

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    
    return df


def qry_book_det(p_ccode=None)  -> pd.DataFrame:
    
    
    """ This function retrieves some summery stats on referee participation in World Cup and returns a df"""   

    import sqlite3
    import pandas as pd 
    conn = sqlite3.connect('data/wcdbmen.db')
    ccode = p_ccode
    #def qry_ref_stats(p_ccode=None):

    #build parameter into sqlstring
    if ccode == None:
        extra=''
    else:
        extra = f"where something != '{ccode}'" 


    #sql_query = f"""SELECT * FROM tournaments WHERE year={my_add} """
    #print(sql_query)
    myqry = f"""
    SELECT *
        FROM bookings_dn
      order by tournament_year

    """

    #print(myqry)
    df = pd.read_sql_query(myqry, conn)
    
    return df






############################################################ Plot Functions #########################################

def plot_ref_stats(df: pd.DataFrame, ptype: str = "bar") -> plt.Figure:
    """
    Create a plot with two subplots side by side:
    - The first subplot shows the percentage of total referees by confederation for each tournament year.
    - The second subplot shows the percentage of games refereed by confederation for each tournament year.

    Args:
    df (pd.DataFrame): The DataFrame containing the data to plot.
    ptype (str): The type of chart to build


    Returns:
    plt (matplotlib.pyplot): The plot object.
    """

    if ptype == 'bar':


        # Prepare the data for 100% stacked bar chart for 'pc_total_refs'
        df_total_refs = df.pivot_table(index='year', columns='confederation_code', values='pc_total_refs_tourn', fill_value=0)

        # Prepare the data for 100% stacked bar chart for 'pc_game_reffed'
        df_game_reffed = df.pivot_table(index='year', columns='confederation_code', values='pc_game_reffed', fill_value=0)

        # Set up the figure and two subplots side by side
        fig, axes = plt.subplots(1, 2, figsize=(20, 5), sharex=True)

        # Colors for different confederations
        colors = plt.get_cmap('tab20').colors

        # First subplot: 100% stacked bar chart of total referees by confederation
        bottom = pd.Series([0] * len(df_total_refs), index=df_total_refs.index)
        for i, conf in enumerate(df_total_refs.columns):
            axes[0].bar(
                df_total_refs.index, 
                df_total_refs[conf], 
                bottom=bottom, 
                color=colors[i], 
                label=conf
            )
            bottom += df_total_refs[conf]

        # Add titles and labels for the first subplot
        axes[0].set_title('Percentage of Total Referees by Confederation per Tournament Year', fontsize=14)
        axes[0].set_xlabel('Year', fontsize=11)
        axes[0].set_ylabel('(%) of Total Referees', fontsize=11)
        axes[0].legend(title='Confederation', bbox_to_anchor=(1.05, 1), loc='upper left')
        axes[0].tick_params(axis='x', rotation=45)  # Rotate x-axis labels

        # Second subplot: 100% stacked bar chart of matches refereed by confederation
        bottom = pd.Series([0] * len(df_game_reffed), index=df_game_reffed.index)
        for i, conf in enumerate(df_game_reffed.columns):
            axes[1].bar(
                df_game_reffed.index, 
                df_game_reffed[conf], 
                bottom=bottom, 
                color=colors[i], 
                label=conf
            )
            bottom += df_game_reffed[conf]

        # Add titles and labels for the second subplot
        axes[1].set_title('Percentage of Matches Refereed by Confederation per Tournament Year', fontsize=14)
        axes[1].set_xlabel('Year', fontsize=11)
        axes[1].set_ylabel('(%) of Matches Refereed', fontsize=11)
        axes[1].legend(title='Confederation', bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        axes[1].tick_params(axis='x', rotation=45)  # Rotate x-axis labels

        # Adjust layout to prevent overlap and improve appearance
        fig.tight_layout()

        # Show the combined plot
        return plt
    
    elif  ptype == 'lin':


        # Set up the figure and two subplots side by side
        fig, axes = plt.subplots(1, 2, figsize=(20, 5), sharex=True)

        # First subplot: Percentage of total referees by confederation for each tournament year
        sns.lineplot(
            data=df, 
            x='year', 
            y='pc_total_refs_tourn', 
            hue='confederation_code', 
            ax=axes[0]
        )

        # Add titles and labels for the first subplot
        axes[0].set_title('Percentage of Total Referees by Confederation per Tournament Year', fontsize=14)
        axes[0].set_xlabel('Year', fontsize=11)
        axes[0].set_ylabel('(%) of Total Referees', fontsize=11)
        axes[0].tick_params(axis='x', rotation=45)  # Rotate x-axis labels

        # Second subplot: Percentage of games refereed by confederation for each tournament year
        sns.lineplot(
            data=df, 
            x='year', 
            y='pc_game_reffed', 
            hue='confederation_code', 
            ax=axes[1]
        )

        # Add titles and labels for the second subplot
        axes[1].set_title('Percentage of Matches Refereed by Confederation per Tournament Year', fontsize=14)
        axes[1].set_xlabel('Year', fontsize=11)
        axes[1].set_ylabel('(%) of Matches Refereed', fontsize=11)
        axes[1].tick_params(axis='x', rotation=45)  # Rotate x-axis labels
        # Adjust layout to prevent overlap and improve appearance
        fig.tight_layout()

        # Return the plot object
        return plt
    
    
def plot_100_single(df):
        
        # Pivot the data to get it in a suitable format for stacked bar plot
        df_pc_game_piv = df.pivot_table(index='year', columns='confederation_code', values='pc_game_reffed', fill_value=0)

        # Plotting the 100% stacked bar chart
        df_pc_game_piv.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='tab20')

        # Add titles and labels
        plt.title('Percentage of Games Reffed by Confederation per Tournament Year (100% Stacked)', fontsize=16)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Percentage of Games Reffed (%)', fontsize=12)

        return plt



def plot_tourn_ratio_mr(df):
    #qry_book_dn_conf()
    # Create a figure with two subplots side by side
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Line plot for counts
    #sns.lineplot(ax=axes[0], x='year', y='count_teams', data=df, label='No Teams', marker='o')
    sns.lineplot(ax=axes[0], x='year', y='count_matches', data=df, label='No Matches', marker='o', color='purple')
    sns.lineplot(ax=axes[0], x='year', y='count_refs', data=df, label='No Refs', marker='o', color='black')


    axes[0].set_title('No. Matches/Refs per Tournament Year', fontsize=14)
    axes[0].set_xlabel('Year', fontsize=12)
    axes[0].set_ylabel('No.', fontsize=12)
    axes[0].legend(title='Refs/Matches', fontsize=10)
    axes[0].grid(True)
    axes[0].tick_params(axis='x', rotation=45) 



    plt.xticks(rotation=90)

    axes[1].set_title('Ratio Comparison of avg Ref Games and Cards Given', fontsize=14)
    # Line plot for averages
    sns.lineplot(ax=axes[1], x='year', y='num_yellow_rat', data=df, label=f"No. Yellow Cards (Ratio'd)", marker='o',color='yellow')
    sns.lineplot(ax=axes[1], x='year', y='ref_game_ratio', data=df, label=f"Games per Ref (Ratio'd)", marker='o',color='black')
    #sns.lineplot(ax=axes[1], x='year', y='num_red', data=df, label='No. Red Card', marker='o')
    sns.lineplot(ax=axes[1], x='year', y='num_red', data=df, label='No. Red Card', marker='o', color='red') 

    axes[1].set_xlabel('Year', fontsize=12)
    axes[1].set_ylabel('No.', fontsize=12)
    #axes[1].invert_yaxis() 
    axes[1].grid(True)
    axes[1].tick_params(axis='x', rotation=45) 
    # Adjust layout
    plt.tight_layout()


def ref_plot_side2(df):

    import seaborn as sns
   # import pandas as pd
    # Set up the figure and two subplots side by side
    fig, axes = plt.subplots(1, 2, figsize=(20, 5), sharex=True)

    # First subplot: Percentage of total referees by confederation for each tournament year
    sns.lineplot(
        data=df, 
        x='year', 
        y='pc_total_refs_tourn', 
        hue='confederation_code', 
        ax=axes[0]
    )

    # Add titles and labels for the first subplot
    axes[0].set_title('Percentage of Total Referees by Confederation per Tournament Year', fontsize=14)
    axes[0].set_xlabel('Year', fontsize=11)
    axes[0].set_ylabel('(%) of Total Referees', fontsize=11)

    # Second subplot: Percentage of games refereed by confederation for each tournament year
    sns.lineplot(
        data=df, 
        x='year', 
        y='pc_game_reffed', 
        hue='confederation_code', 
        ax=axes[1]
    )

    # Add titles and labels for the second subplot
    axes[1].set_title('Percentage of Matches Refereed by Confederation per Tournament Year', fontsize=14)
    axes[1].set_xlabel('Year', fontsize=11)
    axes[1].set_ylabel('(%) of Matches Refereed', fontsize=11)

    # Adjust layout to prevent overlap and improve appearance
    fig.tight_layout()
    return plt



def plot_100bar_refs(data: pd.DataFrame):


    # Prepare the data for 100% stacked bar chart for 'pc_total_refs'
    df_total_refs = df.pivot_table(index='year', columns='confederation_code', values='pc_total_refs_tourn', fill_value=0)

    # Prepare the data for 100% stacked bar chart for 'pc_game_reffed'
    df_game_reffed = df.pivot_table(index='year', columns='confederation_code', values='pc_game_reffed', fill_value=0)

    # Set up the figure and two subplots side by side
    fig, axes = plt.subplots(1, 2, figsize=(20, 5), sharex=True)

    # Colors for different confederations
    colors = plt.get_cmap('tab20').colors

    # First subplot: 100% stacked bar chart of total referees by confederation
    bottom = pd.Series([0] * len(df_total_refs), index=df_total_refs.index)
    for i, conf in enumerate(df_total_refs.columns):
        axes[0].bar(
            df_total_refs.index, 
            df_total_refs[conf], 
            bottom=bottom, 
            color=colors[i], 
            label=conf
        )
        bottom += df_total_refs[conf]

    # Add titles and labels for the first subplot
    axes[0].set_title('Percentage of Total Referees by Confederation per Tournament Year', fontsize=14)
    axes[0].set_xlabel('Year', fontsize=11)
    axes[0].set_ylabel('(%) of Total Referees', fontsize=11)
    axes[0].legend(title='Confederation', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Second subplot: 100% stacked bar chart of matches refereed by confederation
    bottom = pd.Series([0] * len(df_game_reffed), index=df_game_reffed.index)
    for i, conf in enumerate(df_game_reffed.columns):
        axes[1].bar(
            df_game_reffed.index, 
            df_game_reffed[conf], 
            bottom=bottom, 
            color=colors[i], 
            label=conf
        )
        bottom += df_game_reffed[conf]

    # Add titles and labels for the second subplot
    axes[1].set_title('Percentage of Matches Refereed by Confederation per Tournament Year', fontsize=14)
    axes[1].set_xlabel('Year', fontsize=11)
    axes[1].set_ylabel('(%) of Matches Refereed', fontsize=11)
    axes[1].legend(title='Confederation', bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # Adjust layout to prevent overlap and improve appearance
    fig.tight_layout()

    # Show the combined plot
    return plt