import org.neo4j.driver.*;
public class Neo4jInspectNow {
  public static void main(String[] args) {
    Driver driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", AuthTokens.basic("neo4j", "wx123456.."));
    try (Session s = driver.session()) {
      var rs1 = s.run("MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS cnt ORDER BY label");
      System.out.println("LABELS");
      while (rs1.hasNext()) { var r = rs1.next(); System.out.println(r.get(0).asString()+"="+r.get(1).asLong()); }
      var rs2 = s.run("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS cnt ORDER BY t");
      System.out.println("RELS");
      while (rs2.hasNext()) { var r = rs2.next(); System.out.println(r.get(0).asString()+"="+r.get(1).asLong()); }
      var rs3 = s.run("MATCH (src:Field)-[:DIRECT_DERIVES_TO]->(dst:Field) WHERE src.tableName = $src AND dst.tableName = $dst RETURN count(*) AS c", Values.parameters("src", "hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_store_franchisee_profit_share_full_1d", "dst", "hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_share_data_full_1d"));
      System.out.println("DW_TO_DM=" + rs3.single().get("c").asLong());
    } finally { driver.close(); }
  }
}
