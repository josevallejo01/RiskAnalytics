#' Mora ajustada por cosechas a un nivel de madurez por segmentos
#'
#' Construye el cuadro de mora ajustada por cosechas dado un nivel de
#' madurez y consolida un resumen por segmentos. La función incluye la posibilidad de agrupar los meses de
#' desembolso en periodos de más de un mes con el uso del parámetro "periodo".
#' Se asume que el último periodo de análisis disponible es un mes anterior a la fecha
#' en que se hace la consulta.
#' Además del marco de datos con información de cosechas, se genera una tabla
#' con el detalle de desembolsos manteniendo la misma estructura.
#' @param handle integer: Objeto de clase RODBC que establece la conexión al servidor.
#' @param dfBase data.frame: Marco de datos conteniendo como mínimo el código de crédito (NUM_CRE) y el segmento de análisis.
#' @param cFecIni character: Fecha de inicio del periodo total de análisis.
#' @param cFecFin character: Fecha de fin del periodo total de análisis.
#' @param cSegmento character: Nombre del segmento a analizar. Debe encontrarse en dfBase.
#' @param nMadurez numeric: Número de meses desde el mes desembolso, del cual se desea obtener la mora. Por defecto es 6.
#' @param periodo numeric: Número de meses de agrupación para formar grupos temporales. Por defecto es 1.
#' @param batch numeric: Tamaño del lote. Por defecto es 1,500.
#' @param bExportTab logical: Indica si el marco de datos resultante debe ser exportado en un fichero.
#' @param cNomTab character: En caso se exporte el marco de datos, nombre del fichero.
#' @keywords SQL, RODBC, Cosechas
#' @import dplyr
#' @export mxMoraCosechaSegmentoMadurez
#' @return data.frame
#' @examples
#' mora <- mxMoraCosechaSegmentoMadurez(handle = dbhandle,
#'                                      dfBase = data.frame(NUM_CRE = c("001478000088","101478000088"),
#'                                                          PRODUCTO = c("PYME", "CONSUMO")),
#'                                      cFecIni = "2017-01-01",
#'                                      cFecFin = "2017-12-31",
#'                                      cSegmento = "PRODUCTO",
#'                                      nMadurez = 5
#'                                      periodo = 3)

mxMoraCosechaSegmentoMadurez <- function(handle, dfBase,  cFecIni, cFecFin,
                                         cSegmento, nMadurez = 6,
                                         nPeriodo, batch = 1500){
  library(dplyr)
  dfSegmento <- dfBase %>% select(NUM_CRE,cSegmento) %>% distinct()
  dfSegmento <- na.omit(dfSegmento)

  # Lista de segmentos a analizar
  vSegmento <- as.character(unlist(as.list(dfSegmento %>% select(cSegmento)%>% distinct()),
                                   use.names = FALSE))

  dfCosecha <- data.frame()
  dfMontoDesemSegmento <- data.frame()

  vSegmento_final <- NA
  for(i in 1:length(vSegmento)){
    # Maneja errores cuando la función trae data vacía
    # Extrae la cosecha por cada segmento

    temp <- tryCatch(mxMoraCosecha(handle = handle,
                                   vNumCre = unique(dfSegmento[dfSegmento[,2] == vSegmento[i],1]),
                                   cFecIni, cFecFin,
                                   periodo = nPeriodo, batch = batch, bExportTab = FALSE),
                     error = function(e) {
                       print(paste0("Error durante ejecución de mxMoraCosecha para i = ",i,", segmento: ", vSegmento[i]))
                       return(NA)
                     })

    if(!is.na(temp)[1]){
      # Construye tabla de desembolsos por segmentos
      dfMontoDesemSegmento <- bind_rows(dfMontoDesemSegmento,dfMontoDesem)

      # Extrae la madurez requerida de la cosecha por cada segmento
      vSegmento_final[i] <- vSegmento[i]

      if(i == 1) {dfCosecha <- temp[nMadurez,]}
      else{dfCosecha <- bind_rows(dfCosecha,temp[nMadurez,])}
    }

  }
  # Agrega columna con nombres de segmentos
  dfCosecha <- bind_cols(data.frame(c1 = na.omit(vSegmento_final)), dfCosecha)
  dfCosecha <- dfCosecha[!is.na(dfCosecha$MES),]
  names(dfCosecha)[1] <- cSegmento

  dfMontoDesemSegmento <- bind_cols(select(cSegmento, .data = dfCosecha),
                                    dfMontoDesemSegmento)

  assign(x = 'dfMontoDesemSegmento',
         value = dfMontoDesemSegmento,
         envir=.GlobalEnv)

  return(dfCosecha)
}
