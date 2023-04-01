<h1>Airflow DAG: podcast_summary2</h1>
<p>This Airflow DAG named <code>podcast_summary2</code> is designed to extract, transform and load podcast episodes from
    the Marketplace podcast feed, and store them in a SQLite database. It also downloads the audio files of the podcast
    episodes to the specified directory.</p>
<h2>Requirements</h2>
<ul>
    <li>Airflow</li>
    <li>requests library</li>
    <li>xmltodict library</li>
    <li>SQLite</li>
</ul>
<h2>DAG Configuration</h2>
<ul>
    <li><code>dag_id</code>: podcast_summary2</li>
    <li><code>description</code>: podcasts</li>
    <li><code>start_date</code>: March 15, 2023</li>
    <li><code>schedule_interval</code>: Daily</li>
    <li><code>catchup</code>: False</li>
</ul>
<h2>Tasks</h2>
<h3>create_table_sqlite</h3>
<p>This task creates a table named <code>episodes</code> in the SQLite database, with columns <code>link</code>,
    <code>title</code>, <code>filename</code>, <code>published</code>, and <code>description</code>.
</p>
<h3>get_episodes</h3>
<p>This task retrieves the XML data from the Marketplace podcast feed, and parses it to extract the podcast episodes.
    The episodes are returned as a list.</p>
<h3>load_episodes</h3>
<p>This task loads the new episodes into the SQLite database. It checks if an episode already exists in the database by
    comparing its link, and only inserts new episodes. The task also generates the filename of the episode and inserts
    it into the <code>filename</code> column.</p>
<h3>download_episodes</h3>
<p>This task downloads the audio files of the podcast episodes to the specified directory. It iterates over the list of
    episodes, and downloads the audio file for each episode if it does not already exist in the specified directory.</p>
<h2>Note</h2>
<p>This DAG assumes that a SQLite database connection named <code>podcasts</code> has been created in Airflow, and the
    path of the database file is <code>~/airflow/dags/episodes.db</code>. The downloaded audio files will
    be stored in <code>/~/airflow/dags/episodes/</code>. You may need to modify the file paths in the code
    to match your environment.</p>
</div>
