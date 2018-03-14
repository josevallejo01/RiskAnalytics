#' Buscador de segmentos que presentan degradación continua en el indicador de mora ajustada por cosechas
#'
#' Construye un cuadro con los segmentos que han superado un umbral (nTolerancia) de mora 
#' de manera continua en un conjunto de meses (nNumPerTol). La búsqueda se hace a lo largo
#' de una ventana temporal de observación (nOrder). 
#' La búsqueda sigue un algoritmo combinatorio. Dado un vector de segmentos a analizar (vSegmento)
#' y un número establecido de segmentos a tomar en cuenta para la formación de cada combinación,
#' se construye un conjunto de combinaciones de segmentos en los que se
#' buscará que se cumplan los criterios mencionados anteriormente. De especificarse que las combinaciones
#' sean acumulativas (bCumDepth), se construirá progresivamente un conjunto de combinaciones que contendrá
#' inicialmente a cada segmentos hasta contener a las combinaciones entre todos los segmentos.
#' Además del marco de datos con información de los resultados de la búsqueda, se genera los data.frame 
#' "dfMontoDesemSegmentos", "dfNumCreDesemSegmentos" con el detalle de desembolsos y número de créditos desembolsados
#' manteniendo la misma estructura. Adicionalmente se genera el data.frame de validación "dfValid" el cual presenta 
#' las posiciones en donde se cumplio con los criterios de búsqueda.
#' @param handle integer: Objeto de clase RODBC que establece la conexión al servidor.
#' @param dfBase data.frame: Marco de datos conteniendo como mínimo el código de crédito (NUM_CRE) y los segmentos de análisis.
#' @param cFecIni character: Fecha de inicio del periodo total de análisis.
#' @param cFecFin character: Fecha de fin del periodo total de análisis.
#' @param vSegmento vector: Conjunto de segmentos a analizar. Deben encontrarse en dfBase.
#' @param nMadurez numeric: Número de meses desde el mes desembolso, del cual se desea obtener la mora. Por defecto es 6.
#' @param nPeriodo numeric: Número de meses de agrupación para formar grupos temporales (mensuales, bimestrales, etc.). Por defecto es 3 (trimestral).
#' @param nDepth numeric: Número de segmentos que debe contener cada combinación. Por defecto es 2.
#' @param nOrder numeric: Tamaño de la ventana de análisis cuyo punto de referencia inicial es el último periodo disponible. Por defecto es 3.
#' @param bCumDepth boolean: TRUE si la cantidad de segmentos a considerar para la construcción de combinaciones debe ser incremental. 
#' El límite está establecido por la variable nDepth. Por defecto es FALSE.
#' @param nTolerancia numeric: Umbral de referencia para la identificación de segmentos de elevado riesgo.
#' @param batch numeric: Tamaño del lote. Por defecto es 1,500.
#' @param bExportTab logical: Indica si el marco de datos resultante debe ser exportado en un fichero.
#' @param cNomTab character: En caso se exporte el marco de datos, nombre del fichero.
#' @keywords SQL, RODBC, Cosechas
#' @import RODBC, dplyr, xtable
#' @export mxHunter
#' @return data.frame
#' @examples
#' 
#' # Carga la base dentro del entorno R
#' base <- mxBase()
#' 
#' tab <- mxMoraCosechaHunter(handle = dbhandle, dfBase = base, 
#'                            cFecIni =  "2016-01-01", cFecFin = "2018-02-28",
#'                            vSegmento = c("cod_ofi_inf", "nivel", "tip_cre_des"),
#'                            nDepth = 2, bCumDepth = TRUE, nTolerancia = 0.018,
#'                            nNumPerTol = 3, nOrder = 10)

mxMoraCosechaHunter <- function(handle, dfBase, cFecIni, cFecFin,
                                vSegmento, nMadurez = 6, nPeriodo = 3, nDepth = 2,
                                nOrder = 3, bCumDepth = FALSE, nTolerancia = 0.18, batch = 1500,
                                bExportTab = FALSE, cNomTab){
  library(RODBC)
  library(dplyr) 
  
  if(nNumPerTol > nOrder){return("nNumPerTol no puede superar nOrder")}
  
  # Segmentos de análisis
  # matCombn <- list(combn(length(vSegmento), nDepth))
  library(zoo)

  if(bCumDepth == TRUE){
    matCombn <- mapply(FUN = function(x, m) combn(x, m),
                       x = length(vSegmento),
                       m = 1:nDepth)
  }else{
    matCombn <- list(combn(length(vSegmento), nDepth))
  }

  dfMontoDesemSegmentos <- data.frame()
  dfNumCreDesemSegmentos <- data.frame()
  dfSegmentos <- data.frame()
  
  for(i in 1:length(matCombn)){
    for(j in 1:ncol(matCombn[[i]])){
      dfSegmentos <- bind_rows(dfSegmentos,
                               mxMoraCosechaSegmentoMadurez(handle = handle,
                                                            dfBase = dfBase,
                                                            cFecIni =  cFecIni,
                                                            cFecFin = cFecFin,
                                                            vSegmento = vSegmento[matCombn[[i]][,j]],
                                                            nMadurez = nMadurez,
                                                            nPeriodo = nPeriodo,
                                                            batch = batch))
      
      dfMontoDesemSegmentos <- bind_rows(dfMontoDesemSegmentos,
                                         dfMontoDesemSegmento)
      
      
      dfNumCreDesemSegmentos <- bind_rows(dfNumCreDesemSegmentos,
                                          dfNumCreDesemSegmento)
      
      #Cambia posición de la última columna si se agrega un nuevo elemento
      if(i == 1 && j > 1){
        dfSegmentos <- dfSegmentos %>% 
          select(vSegmento[matCombn[[i]][j]], everything())
        
        dfMontoDesemSegmentos <- dfMontoDesemSegmentos %>%
          select(vSegmento[matCombn[[i]][j]], everything())
        
        dfNumCreDesemSegmentos <- dfNumCreDesemSegmentos %>%
          select(vSegmento[matCombn[[i]][j]], everything())
      }
    }
  }

  # Limpia la matriz de últimas columnas NA
  l<- apply(dfSegmentos,2,function(x)any(!is.na(x)))
  l <- l[l]
  l <- names(l[length(l)])
  
  dfSegmentos <- dfSegmentos[,1:match(l,names(dfSegmentos))]

  # Cancela si el número de columnas es menor al  periodos de análisis
  if(ncol(dfSegmentos[,(length(vSegmento)+2):ncol(dfSegmentos)]) < nNumPerTol ||
     ncol(dfSegmentos[,(length(vSegmento)+2):ncol(dfSegmentos)]) < nOrder){
    return("el número de columnas de análisis no puede ser menor al periodo o ventana de análisis")}

  # Toma la ventana de análisis nOrder
  k <- dfSegmentos[,(ncol(dfSegmentos)-nOrder+1):ncol(dfSegmentos)]
  
  # Revisa segmentos que sobrepasaron la tolerancia contínuamente por un número de
  # periodos nNumPerTol = 3
  p <- apply(k, 1,
             function(nNumPerTol){
               rollapply(nNumPerTol, 3, function(x)all(x>nTolerancia ))}
  )

  # Da formato a la matriz de validación
  if(nNumPerTol == nOrder){
    p[is.na(p)] <- FALSE
    dfValid <- bind_cols(dfSegmentos[,1:(length(vSegmento)+1)],data.frame(X1 = p))
    dfValid <- dfValid[p,]
    dfFinal <- bind_cols(dfSegmentos[,1:(length(vSegmento)+1)],k)[p,]
  }else{
    dfValid <- bind_cols(dfSegmentos[,1:(length(vSegmento)+1)],
                         k,
                         data.frame(t(p)))
    # Elimina filas que no cumplen con la condición
    g <- apply(dfValid[,(length(vSegmento)+2+nOrder):ncol(dfValid)],
               1,
               function(x)all(x == FALSE))
    g[is.na(g)] <- TRUE
    
    dfValid <- dfValid[!g,]
    dfFinal <- dfValid[,1:(length(vSegmento)+1+nOrder)]
    dfValid <- bind_cols(dfValid[,1:(length(vSegmento)+1)],
                         dfValid[,(length(vSegmento)+2+nOrder):ncol(dfValid)])
  }
  
  # Genera tablas finales de desembolsos y números operaciones
  dfMontoDesemSegmentos <- dfMontoDesemSegmentos[as.numeric(row.names(dfValid)),]
  dfMontoDesemSegmentos <- dfMontoDesemSegmentos[,c(1:(length(vSegmento)),
                           (match(l,names(dfMontoDesemSegmentos))-nDepth):
                             ncol(dfMontoDesemSegmentos))]
  
  dfNumCreDesemSegmentos <- dfNumCreDesemSegmentos[as.numeric(row.names(dfValid)),]
  dfNumCreDesemSegmentos <- dfNumCreDesemSegmentos[,c(1:(length(vSegmento)),
                                                    (match(l,names(dfNumCreDesemSegmentos))-nDepth):
                                                      ncol(dfNumCreDesemSegmentos))]
  
  if(bExportTab == TRUE){
    library(xtable)
    table <- xtable(x = dfFinal,
                    digits = 2,
                    latex.environments="center")
    
    # Crea directorio
    mainDir <- "./"
    subDir <- "TABLAS"
    
    ifelse(!dir.exists(file.path(mainDir, subDir)),
           dir.create(file.path(mainDir, subDir)), FALSE)
    
    print(table,
          type = "html",
          include.rownames = FALSE,
          file = paste0("./TABLAS/", cNomTab, ".html"))
  }
  
  
  assign(x = 'dfMontoDesemSegmentos',
         value = dfValid,
         envir=.GlobalEnv)
  
  assign(x = 'dfNumCreDesemSegmentos',
         value = dfValid,
         envir=.GlobalEnv)
  
  assign(x = 'dfValid',
         value = dfValid,
         envir=.GlobalEnv)
  
  return(dfFinal)

}
