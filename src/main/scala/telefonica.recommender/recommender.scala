package telefonica.recommender

/********************************************************************************************************************************************/
/********************************************************************************************************************************************/
/********************************************************************************************************************************************/
/********************************************************************************************************************************************/
/**************************** ******************************************/
/****************************                                                                      ******************************************/
/****************************                                                                      ******************************************/
/****************************                                                                      ******************************************/
/****************************                                                                      ******************************************/
/********************************************************************************************************************************************/
/********************************************************************************************************************************************/
/********************************************************************************************************************************************/

/*********************************/
/*** Se importan dependencias. ***/
/*********************************/

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.split
import org.apache.hadoop.fs.{FileSystem, Path}

/********************************************************/
/*** Se crean clases para parsear archivos de entrada ***/
/********************************************************/

case class RowHeaderHDR( file_name:String, timestamp:String)		// Crea una clase que tiene la definición de campos del header (file_name y timestamp).
case class RowStores( store_type:String, url:String)				// Crea una clase que tiene la definición de campos del insumo de tiendas online.


/*******************************/
/*** Inicio  DPI_Recommender ***/
/*******************************/

object DPI_Recommender {
  def main(args: Array[String]) {
  
	/**************************************************************************/
    /*** Se setean variables de configuración de Spark Context y sqlContext ***/
    /**************************************************************************/

    val conf = new SparkConf().setAppName("Recommender")
    val sc = new SparkContext(conf)
	val sqlContext= new org.apache.spark.sql.SQLContext(sc)
	import sqlContext.implicits._								// Sólo se puede importar una vez se haya definido sqlContext.
	val fs = FileSystem.get(sc.hadoopConfiguration)
	
	/******************************************************/
    /*** Se eliminan directorios y archivos temporales. ***/
    /******************************************************/

	val DeletePath = new Path("/desarrollo/apps/dpi/recommender/tmp/HDR_tmp")
	if (fs.exists(DeletePath))
       fs.delete(DeletePath, true)
	   
	/****************************************************************/
    /*** Se generan RDD y DataFrames a partir de archivos insumo. ***/
    /****************************************************************/

    val xdr = sc.textFile("/desarrollo/apps/dpi/recommender/input/HDR_SDE-2-VM1-RG-MOVSIB_20200207T000000_20200207T150000.csv")	// Importa archivo de entrada de tráfico
    val RDD_xdr_headerfooter = xdr.filter(_.startsWith("HDR"))															// Filtra líneas que no comiencen por la cadena de texto "HDR" y se guarda en un RDD.
    // val RDD_xdr = xdr.filter(!_.contains("HDR"))																				  // Filtra líneas que no contengan la cadena de texto "HDR" y se guarda en un RDD.
    val RDD_xdrTmp = xdr.filter(!_.startsWith("HDR"))                                       // Filtra líneas que no comiencen por la cadena de texto "HDR" y se guarda en un RDD. Esta data corresponde a los HDR.
    val RDD_xdrTmp2 = RDD_xdrTmp.map(_.replace("www.",""))                                  // Reemplaza la cadena "www." por nada. Esto para que cruce con el archivo de ecommerce.
    val RDD_xdr = RDD_xdrTmp2.map(_.replace("%2F","/"))                                     //Reemplaza la cadena "%2F" por "/". Esto debido para tener la url en formato correcto para que pueda ser accedida.
    RDD_xdr.coalesce(1).saveAsTextFile("/desarrollo/apps/dpi/recommender/tmp/HDR_tmp")      // Se guardan los HDRs limpios en un archivo temporal para que luego puedan ser cargados en un DataFrame.
    val stores = sc.textFile("/desarrollo/apps/dpi/recommender/param/ecommerce_list.txt")
	val brands = sc.textFile("/desarrollo/apps/dpi/recommender/param/phone_brands.txt")
	
	/***********************************************************/
    /*** Se crean funciones para parsear archivos de entrada ***/
    /***********************************************************/

    
    def parseRowHeaderHDR( arr:Array[String]) = RowHeaderHDR( arr(0), arr(1))	// Crea una función que reciba un arreglo y lo mapee a la estructura del header.   
    def parseRowStores( arr:Array[String]) = RowStores( arr(0), arr(1))			// Crea una función que reciba un arreglo y lo mapee a la estructura del insumo de tiendas online.
	
	/************************************************/
    /*** Se crea DataFrame que contiene el header ***/
    /** Primero se parsean las líneas que comiencen */
    /** por la cadena "HDR" por separador pipe ("|")*/
    /** Luego se ordenan las dos líneas para que la */
    /** primera que quede sea el header (por fecha) */
    /** Luego se toma el primer registro que corres */
    /** ponde al header.                            */
    /************************************************/

    val df_xdr_headerfooterTmp = xdr.filter(_.startsWith("HDR")).map( _.split('|')).map( arr => parseRowHeaderHDR( arr )).toDF
    val df_xdr_headerfooter = df_xdr_headerfooterTmp.withColumn("rownum", row_number().over(Window.partitionBy("file_name").orderBy("timestamp")))
    val df_xdr_header = df_xdr_headerfooter.select("file_name", "timestamp").filter("rownum == 1")
	
	/************************************************/
    /*** Se crea DataFrame que contiene el footer ***/
    /** Primero se crea un PairRDD cuyo key es la   */
    /** longitud de la cadena. La cadena que tiene  */
    /** menos de 80 caracteres corresponde al header*/
    /** ya que el header solo trae dos campos. El   */
    /** footer siempre tiene más de 80 caracteres.  */
    /** Luego se filtra y parsea el footer para gene*/
    /** rar los tres campos: file_name, qty_rows,   */
    /** timestamp.                                  */
    /************************************************/

    val df_xdr_headerfooterLength = RDD_xdr_headerfooter.keyBy(_.length).toDF
    val df_xdr_footerTmp = df_xdr_headerfooterLength.select("_2").filter("_1 >= 80")

    val df_xdr_footer =
    df_xdr_footerTmp.withColumn("_tmp", split($"_2", "\\|")).select(
      $"_tmp".getItem(0).as("file_name"),
      $"_tmp".getItem(1).as("qty_rows"),
      $"_tmp".getItem(2).as("timestamp")
    ).drop("_tmp")
	
	/******************************************************************/
    /*** Se crea DataFrame que contiene el insumo de tiendas online ***/
    /******************************************************************/

    val df_stores = stores.map( _.split(',')).map( arr => parseRowStores( arr )).toDF
    val df_url_stores = df_stores.select("url")
	
	/*************************************************************/
    /*** Se crea DataFrame que contiene los HDRs estructurados ***/
    /** Este se crea con base en el archivo de HDRs exportado en */
    /** la segunda sección. Luego se cruza con las tiendas ya que*/
    /** sólo nos interesan los HDRs asociadas a estas.           */
    /*************************************************************/

    val df_hdr = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").option("header","false").option("quote", "\"").option("delimiter", "|").load("/desarrollo/apps/dpi/recommender/tmp/HDR_tmp/part-00000").toDF(
	"Start_timestamp",
    "Stop_timestamp",
    "Report_Trigger",
    "IMEI",
    "IMSI",
    "MSISDN",
    "UE_IP_Address_IPv4",
    "UE_IP_Address_IPv6",
    "SGSN_SGW_IP",
    "GGSN_PGW_IP",
    "eNodeB_IP",
    "Service_Destination_IP_Address",
    "MCC",
    "MNC",
    "LAC",
    "RAC",
    "SAC",
    "TAC",
    "Cell_Id",
    "NSAPI",
    "EPS_Bearer_Id",
    "RAT_Type",
    "Session_Id",
    "APN",
    "Source_Id",
    "Hostname",
    "Service_Port",
    "Resource",
    "HTTP_User_Agent",
    "HTTP_Referer",
    "Application_Id",
    "Service_Protocol",
    "Application_Protocol",
    "Application_Type",
    "Application_Method",
    "Application_Method_Cause",
    "HTTP_Delay",
    "RTT_Server",
    "RTT_Client",
    "HTTP_Rx_Bytes",
    "HTTP_Tx_Bytes",
    "Content_Type")

    val df_hdr_stores = df_hdr.join(
        df_url_stores
    , col("Hostname") === col("url")
    , "inner")


  }
 }
