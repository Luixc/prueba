######INSTALACION DE LIBRERIAS#############
#install.packages("rjson")
#install.packages("DBI")
#install.packages("RPostgreSQL")

#LLAMADO DE LIBRERIAS
library(RPostgreSQL)
library(DBI)
library(rjson)

#DEFINICION DE DRIVER DE POSTGRES
pg = dbDriver("PostgreSQL")

#CONECCION A LA BD 
con = dbConnect(pg, user="seratic", password="seratic2018",
                host="test.cluster-ro-cjmcnfeqjnfn.us-east-1.rds.amazonaws.com", port=5432, dbname="ed_suite")

#SELECCION DE ESQUEMA
selected  <-  "  set search_path = 'db_304'  "
timeserie  <- dbGetQuery ( con , selected )


#CONSULTA SQL 
selecbody <- "  select
cast(row_to_json(endt) as text)
from
(
  select
  *
  from
  (
  select
  jsonb_agg(jsonb_build_object('nro_doc_titular', cotizacion.solicitante__nro_doc_titular)|| jsonb_build_object('primer_nombre', cotizacion.solicitante__primer_nombre, 'id_publico', cotizacion.solicitante__id_publico , 'id', cotizacion.solicitante__id )) as solicitante, cotizacion.id_solicitante, cotizacion.id, cotizacion.id_publico, cotizacion.habilitado, cotizacion.borrado, cotizacion.cod_oficina, cotizacion.estado_cotizacion, cotizacion.fecha_creacion_cotizacion
  from
  (
  select
  distinct cotizacion.id_solicitante, cotizacion.id, cotizacion.id_publico, cotizacion.habilitado, cotizacion.borrado, cotizacion.atributos ->> 'cod_oficina' as cod_oficina, cotizacion.atributos ->> 'estado_cotizacion' as estado_cotizacion, cotizacion.atributos ->> 'fecha_creacion_cotizacion' as fecha_creacion_cotizacion, solicitante.atributos ->> 'nro_doc_titular' as solicitante__nro_doc_titular, solicitante.atributos ->> 'primer_nombre' as solicitante__primer_nombre, solicitante.id as solicitante__id , solicitante.id_publico as solicitante__id_publico , jsonb_build_object() as id_usuario_creacion, jsonb_build_object() as id_usuario_modificacion
  from
  cotizacion
  left join solicitante on
  solicitante.id = cotizacion.id_solicitante
  and ((solicitante.habilitado = true
  and solicitante.borrado = false)
  or solicitante is null )
  where
  (cotizacion.habilitado = true
  and cotizacion.borrado = false) ) as cotizacion
  group by
  cotizacion.id_solicitante, cotizacion.id, cotizacion.id_publico, cotizacion.habilitado, cotizacion.borrado, cotizacion.cod_oficina, cotizacion.estado_cotizacion, cotizacion.fecha_creacion_cotizacion
  order by
  cotizacion.id desc ) as cotizacion
  where
  cotizacion.id is not null
) endt;
  "
#resultado consulta
timeserie  <- dbGetQuery ( con , selecbody ); 

#GUARDO EN RES LO QUE HAY EN LA COLUMNA ROW_TO_JSON
res <- jsonlite::stream_in(textConnection(timeserie$row_to_json)) #stream in: implementa procesamiento linea por linea del json a travez de una conexion

#extraigo solicitante
sol <- res$solicitante
#convierto a dataframe ordenado
df_solicitante <- as.data.frame(do.call(rbind, sol))

#EXTRACCION POR COLUMNAS DE LOS DOS DF 
sol_united <- data.frame(df_solicitante$primer_nombre ,df_solicitante$id, res$id, res$id_publico, res$habilitado, res$borrado, res$cod_oficina, res$estado_cotizacion, res$fecha_creacion_cotizacion)
df_sol_united <- as.data.frame(sol_united)
names(df_sol_united)
names(df_sol_united) = c ("primer_nommbre","id","ID","id_publico","habilitado","borrado","cod_oficiona","estado_cotizacion","fecha_creacion_cotizacion" )

####division de dataframes########

# Division de datos del dataframe, especificando las filas
x = data.frame(df_sol_united, 1:16678)
#NUMERO DE DIVISONES
n <- 2 
#USO DE SPLIT PARA DIVIDIR
df_div <- split(x, factor(sort(rank(row.names(x))%%n))) 
#factor define la agrupación, sort: ordena asendente, rank da un rango, por orden de filas, %% division modular
#df_div[1]
df_div1 <- as.data.frame(df_div[1])
df_div2 <- as.data.frame(df_div[2])
#######################################

#expxortar df en csv especificando el directorio
setwd("/home/luis/Documentos/dirR") 
pum <-"prueba3.csv" #nombre del archivo
write.csv(df_sol_united, file = pum)

#holis



