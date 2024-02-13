-- part of a query repo
-- query name: Trending Words (Mentioned by users of "active" tier or higher)
-- query link: https://dune.com/queries/3418395


with 
    words as (
        with 
        cleaning as (
            SELECT 
            COALESCE(c.parent_url, c.root_parent_url) as parent_url
            , case when c.created_at >= now() - interval '7' day then 1 else 0 end as this_week
            , fid
            -- , split(regexp_extract(text, 'https://\S*'),'/')[3] as link_domains --do popular domains separately later
            , split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(text, 'https://\S*',''),'[[:punct:]]', ''),'0x[a-fA-F0-9]{40}',''),'<[^>]+>',''),' ') as text_cleaned
            , text
            FROM dune.neynar.dataset_farcaster_casts c
            WHERE COALESCE(c.parent_url, c.root_parent_url) is not null
            and deleted_at is null
            and ('{{words channel filter}}' = 'all channels' OR lower(parent_url) LIKE '%' || lower('{{words channel filter}}') || '%')
            and c.created_at >= now() - interval '14' day
            and c.fid IN (SELECT fid FROM query_3418402 WHERE fid_active_tier >=1)
        )
    
        SELECT 
        -- parent_url,
        this_week,
        fid,
        lower(trim(c.word)) as word
        -- , text
        FROM cleaning
        LEFT JOIN unnest(text_cleaned) as c(word) on true
        LEFT JOIN dune.dune.dataset_stopwords st ON lower(trim(c.word)) = regexp_replace(st.stopwords,'[[:punct:]]', '')
        WHERE st.stopwords is null
        and lower(trim(c.word)) not in ('today','like','follow','recast','get','good','join','lets','new','left','back'
                                        ,'great','know','nice','done','need','think','guys','people','thank','hello'
                                        ,'bro','lol','much','really','right','thats','hot','cool','use','yes','no','also'
                                        ,'everyone','something','post','whats','come','many','sure','ill','take'
                                        ,'keep','every','well','ive','even','anyone','always','big','made'
                                        ,'could','using','feel','never','find','ready','thing','followers','better'
                                        ,'let','already','looking','look','say','try','yet','miss','give','coming'
                                        ,'getting','last','yeah','fam','happy','hope','days','start','things','gonna'
                                        ,'another','please','lot','wait','amazing','users','user','around','week','worth'
                                        ,'share','joined','man','following','trying','guy','might','though','since'
                                        ,'waiting','actually','less','anything','seems','haha','makes','may','stuff'
                                        ,'ago','finally','definitely','everything','end','shit','fuck','damn','lmao','bad'
                                        ,'one','first','comment','time','see','got','make','still','want','day','way'
                                        ,'cant','going','would','next','real','cast','reply','click','help','hey','nothing'
                                        ,'free','love','similar','seem','worked','basically','directly','especially','likely'
                                        ,'probably','exactly','totally','fully','currently','usually','absolutely','truly'
                                        ,'recently','simply','literally','via','sir','heres','idk','either','omg','thanks')
        and try(cast(lower(trim(c.word)) as int)) is null
        and length(lower(trim(c.word))) > 2 --get rid of emojis and small words
    )
    
    --tf-idf calculation
    , term_frequency AS (
        SELECT
            this_week,
            fid,
            word,
            COUNT(*) AS tf
        FROM words
        GROUP BY 1,2,3
    )
    
    , document_frequency AS (
      SELECT
        this_week,
        word,
        COUNT(DISTINCT fid) AS df
      FROM words
      GROUP BY 1,2
    )
    
    , total_documents AS (
      SELECT 
        this_week
        , COUNT(DISTINCT fid) AS total_docs
      FROM words
      group by 1
    )
    
    , idf AS (
      SELECT
        df.this_week,
        df.word,
        df.df,
        td.total_docs,
        LOG10(cast(td.total_docs as double) / cast(df.df as double)) AS idf
      FROM document_frequency df 
      LEFT JOIN total_documents td ON td.this_week = df.this_week
    )
    
    , summed as (
        SELECT
            tf.word,
            max(case when tf.this_week = 1 then idf.idf else null end) AS idf,
            max(case when tf.this_week = 0 then idf.idf else null end) AS idf_last,
            sum(case when tf.this_week = 1 then tf.tf else 0 end) AS tf,
            sum(case when tf.this_week = 0 then tf.tf else 0 end) AS tf_last,
            sum(case when tf.this_week = 1 then tf.tf * idf.idf else 0 end) AS tfidf,
            sum(case when tf.this_week = 0 then tf.tf * idf.idf else 0 end) AS tfidf_last
        FROM term_frequency tf
        JOIN idf ON tf.word = idf.word AND tf.this_week = idf.this_week
        group by 1
    )
    
    --nothing to do with tfidf calc
    , casters_week AS (
      SELECT
        word,
        COUNT(DISTINCT case when this_week = 1 then fid else null end) AS casters,
        COUNT(DISTINCT case when this_week = 0 then fid else null end) AS casters_last
      FROM words
      GROUP BY 1
    )

SELECT
row_number() over (order by casters desc) as ranking
, s.word
, w.casters
, w.casters - COALESCE(w.casters_last,0) as casters_wow
, s.tf
, s.tf - COALESCE(s.tf_last,0) as tf_wow
, s.idf
, s.idf - COALESCE(s.idf_last,0) as idf_wow
, s.tfidf
, s.tfidf - COALESCE(s.tfidf_last,0) as tfidf_wow
FROM summed s
LEFT JOIN casters_week w On w.word = s.word
WHERE w.casters >= 10 --mentioned by at least 10 casters so that it isn't capturing just one account spamming something
order by casters desc
LIMIT 1000