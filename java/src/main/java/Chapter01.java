import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static class RedisKeyNameBuilder{
        public static final String scoreSortedSetName = "score:";
        public static final String negativeScoreSortedSetName = "negativeScore:";
        public static final String timeSortedSetName = "time:";
        public static String composeVotedUserSetName(String articleId) {
            return "voted:" + articleId;
        }
        public static String composeArticleHashName(String articleId) {
            return "article:" + articleId;
        }
    }

    public static final void main(String[] args) {
        new Chapter01().run();
    }


    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = postArticle(
            conn, "username", "A title", "http://www.google.com");
        displayArticle(conn, articleId);

        System.out.println();

        articleVote(conn, "other_user", RedisKeyNameBuilder.composeArticleHashName(articleId), true);
        String votes = conn.hget(RedisKeyNameBuilder.composeArticleHashName(articleId), "votes");
        displayVotes(votes);

        System.out.println("The currently positive highest-scoring articles are:");
        List<Map<String,String>> articles = getArticlesWithOrder(conn, 1, RedisKeyNameBuilder.scoreSortedSetName);
        printArticles(articles);
        assert articles.size() >= 1;

        System.out.println("The currently negative highest-scoring articles are:");
        List<Map<String,String>> negativeArticles = getArticlesWithOrder(conn, 1, RedisKeyNameBuilder.negativeScoreSortedSetName);
        printArticles(negativeArticles);


        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        //display article order by score based on group
        negativeArticles = getGroupArticles(conn, "new-group", 1);
        printArticles(negativeArticles);
        assert negativeArticles.size() >= 1;
    }

    private void displayVotes(String votes) {
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;
    }

    private void displayArticle(Jedis conn, String articleId) {
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String,String> articleData = conn.hgetAll(RedisKeyNameBuilder.composeArticleHashName(articleId));
        for (Map.Entry<String,String> entry : articleData.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
    }

    /**
     * voted1/2/3:-->set:user1,user2..
     * article1/2/3 --> hashmap: tiltle...link...
     * score -->zset: time+vote_score(order score)   article1/2/3
     * time -->zset: time(order score) article1/2/3
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        String articleId = String.valueOf(conn.incr("article:"));

        String voted = RedisKeyNameBuilder.composeVotedUserSetName(articleId);
        conn.sadd(voted, user);//voted+articleID(Set)->user1,user2
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = RedisKeyNameBuilder.composeArticleHashName(articleId);
        HashMap<String,String> articleData = new HashMap<String,String>();
        articleData.put("id", article);
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        articleData.put("negativeVotes", "0");
        conn.hmset(article, articleData);//article+articleID(Hash)-> "title"-->"";"link"-->""...
        conn.zadd(RedisKeyNameBuilder.scoreSortedSetName, now + VOTE_SCORE, article);//score:(zset)->
        conn.zadd(RedisKeyNameBuilder.timeSortedSetName, now, article);//time:(zset)->now(score) article(member)

        return articleId;
    }

    public void articleVote(Jedis conn, String user, String article, boolean negative) {
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore(RedisKeyNameBuilder.timeSortedSetName, article) < cutoff){
            return;
        }

        String articleId = article.substring(article.indexOf(':') + 1);
        if (conn.sadd(RedisKeyNameBuilder.composeVotedUserSetName(articleId), user) == 1) {
            if(negative){
                conn.zincrby(RedisKeyNameBuilder.negativeScoreSortedSetName, VOTE_SCORE, article);
                conn.hincrBy(article, "negativeVotes", 1);
            }else{
                conn.zincrby(RedisKeyNameBuilder.scoreSortedSetName, VOTE_SCORE, article);
                conn.hincrBy(article, "votes", 1);
            }
        }
    }


    public List<Map<String,String>> getArticlesWithOrder(Jedis conn, int page, String setName) {
        return getArticles(conn, page, setName);
    }



    public List<Map<String,String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);// desc order
        List<Map<String,String>> articles = new ArrayList<Map<String,String>>();
        for (String id : ids){
            Map<String,String> articleData = conn.hgetAll(id);
            articles.add(articleData);
        }

        return articles;
    }

    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = RedisKeyNameBuilder.composeArticleHashName(articleId);
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, RedisKeyNameBuilder.scoreSortedSetName);
    }

    /**
     * do sorting in group
     */
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page, String sortedScoreKey) {
        String key = sortedScoreKey + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, sortedScoreKey);//将group set和order zset做交集，把交集结果放到新的zset中（key）,并以最大的score作为新的zset的score
            conn.expire(key, 60);//缓存结果60s
        }
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
