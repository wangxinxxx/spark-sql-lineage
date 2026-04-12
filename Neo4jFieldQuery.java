import org.neo4j.driver.*;
import java.util.*;

public class Neo4jFieldQuery {
  public static void main(String[] args) {
    Driver driver = GraphDatabase.driver(
      "neo4j://127.0.0.1:7687",
      AuthTokens.basic("neo4j", "wx123456.."),
      Config.defaultConfig());
    try (Session session = driver.session()) {
      String cypher =
        "MATCH (src:Field)-[:DIRECT_DERIVES_TO]->(dst:Field) " +
        "WHERE dst.tableName = $tableName AND dst.name = $fieldName " +
        "RETURN src.tableName AS srcTable, src.name AS srcField, dst.tableName AS dstTable, dst.name AS dstField " +
        "ORDER BY srcTable, srcField";
      Result rs = session.run(cypher, Values.parameters(
        "tableName", "hdp_ubu_zhuanzhuan_ads_c2b.ads_bi_offline_store_operating_data_center_v3_full_1d",
        "fieldName", "zz_undertake_non_fixed_cost"));
      while (rs.hasNext()) {
        Record r = rs.next();
        System.out.println(r.get("srcTable").asString() + "." + r.get("srcField").asString() +
          " -> " + r.get("dstTable").asString() + "." + r.get("dstField").asString());
      }
    } finally {
      driver.close();
    }
  }
}
