package sparkML

import org.apache.spark.sql.SparkSession

object NetworkAnomalyDetection {

  def main(args: Array[String]): Unit = {

    // the usual spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Anomaly Detection in Network traffic")
      .getOrCreate()

    val data = spark.read
      .option("inferSchema", true)
      .option("header", false)
      .csv("src/main/scala/resources/chapter2/kddcup.data")
      .toDF( "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label")

    data.cache()


  }

}
