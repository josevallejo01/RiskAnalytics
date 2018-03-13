#' Mora ajustada por cosechas a un nivel de madurez por segmentos
#'
#' Construye el cuadro de mora ajustada por cosechas dado un nivel de
#' madurez y consolida un resumen por segmentos. La función incluye la posibilidad de agrupar los meses de
#' desembolso en periodos de más de un mes con el uso del parámetro "periodo".
#' Se asume que el último periodo de análisis disponible es un mes anterior a la fecha
#' en que se hace la consulta.
#' Además del marco de datos con información de cosechas, se generan los data.frame "dfMontoDesemSegmento"
#' y "dfNumCreDesemSegmento" con el detalle de desembolsos y número de créditos desembolsados
#' manteniendo la misma estructura.
#' @param handle integer: Objeto de clase RODBC que establece la conexión al servidor.
#' @param dfBase data.frame: Marco de datos conteniendo como mínimo el código de crédito (NUM_CRE) y el segmento de análisis.
#' @param cFecIni character: Fecha de inicio del periodo total de análisis.
#' @param cFecFin character: Fecha de fin del periodo total de análisis.
#' @param vSegmento vector: Conjunto de segmentos a analizar. Deben encontrarse en dfBase.
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
#'                                      vSegmento = c("PRODUCTO", "RANGO_EDAD"),
#'                                      nMadurez = 5
#'                                      periodo = 3)

mxMoraCosechaSegmentoMadurez <- function(handle, dfBase,  cFecIni, cFecFin,
                                         vSegmento, nMadurez = 6,
                                         nPeriodo, batch = 1500){
  library(dplyr)
  dfSegmento <- dfBase %>% select(NUM_CRE,vSegmento) %>% distinct()
  dfSegmento <- na.omit(dfSegmento)

  # Lista de segmentos a analizar
  tabSegmento <- dfSegmento %>%
    group_by_at(vSegmento) %>%
    summarise(nNumCre = n_distinct(NUM_CRE)) %>%
    mutate_all(as.character)


  dfCosecha <- data.frame()
  dfMontoDesemSegmento <- data.frame()
  dfNumCreDesemSegmento <- data.frame()
  dfSegmentoFinal <- data.frame()

  for(i in 1:nrow(tabSegmento)){
    # Selecciona las filas que cumplen con el segmento i
    dfNumCre <- inner_join(dfSegmento, tabSegmento[i,-ncol(tabSegmento)],
                           by = colnames(tabSegmento[i,-ncol(tabSegmento)]))

    dfNumCre <- na.omit(dfNumCre)
    # Maneja errores cuando la función trae data vacía
    # Extrae la cosecha por cada segmento
    temp <- tryCatch(mxMoraCosecha(handle = handle,
                                   vNumCre = unique(dfNumCre$NUM_CRE),
                                   cFecIni, cFecFin,
                                   periodo = nPeriodo, batch = batch, bExportTab = FALSE),
                     error = function(e) {
                       print(paste0("Error durante ejecución de mxMoraCosecha para i = ",
                                    i,", ", names(tabSegmento), ": ",
                                    tabSegmento[i,]))
                       return(NA)
                     })

    if(!is.na(temp)[1]){
      # Construye tabla de desembolsos por segmentos
      dfMontoDesemSegmento <- bind_rows(dfMontoDesemSegmento,dfMontoDesem)

      # Construye tabla de número de créditos desembolsados por segmento
      dfNumCreDesemSegmento <- bind_rows(dfNumCreDesemSegmento,dfNumCreDesem)

      # Extrae la madurez requerida de la cosecha por cada segmento
      dfSegmentoFinal <- rbind(dfSegmentoFinal,
                               data.frame(tabSegmento[i,-ncol(tabSegmento)]))

      if(i == 1) {dfCosecha <- temp[nMadurez,]}
      else{dfCosecha <- bind_rows(dfCosecha,temp[nMadurez,])}
    }

  }

  # Agrega columna con nombres de segmentos
  dfCosecha <- bind_cols(dfSegmentoFinal, dfCosecha)
  dfCosecha <- dfCosecha[!is.na(dfCosecha$MES),]

  dfMontoDesemSegmento <- bind_cols(dfSegmentoFinal, dfMontoDesemSegmento)

  dfNumCreDesemSegmento <- bind_cols(dfSegmentoFinal, dfNumCreDesemSegmento)

  # Agrega tablas de desembolsos y número de créditos desembolsados al ambiente global
  assign(x = 'dfMontoDesemSegmento',
         value = dfMontoDesemSegmento,
         envir=.GlobalEnv)

  assign(x = 'dfNumCreDesemSegmento',
         value = dfNumCreDesemSegmento,
         envir=.GlobalEnv)

  return(dfCosecha)
}
