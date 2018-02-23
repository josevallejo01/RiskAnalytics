#' Realiza consultas en lotes a una conexión SQL
#'
#' La función realiza el requerimiento de datos para un vector de elementos
#' a una conexión de base de datos utilizando la cláusula "WHERE ... IN".
#' Cuando el tamaño de vector es significativo (aproximadamente mayor
#' a 30,000 observaciones) la consulta se hace compleja y se rechaza
#' la solicitud.
#' Para afrontar esta situación se divide el vector en lotes y se realiza
#' la consulta en bucle mientras los resultados se unen en la tabla
#' que será retornada.
#'
#' @param handle integer: Objeto de clase RODBC que establece la conexión al servidor.
#' @param vcListaRef vector: Conjunto de elementos a ser consultados en base de datos.
#' @param cNombreRef character: Nombre de la columna en la tabla que contiene
#' el conjunto de elementos que serán consultados.
#' @param cSelect character: Cadena de texto correspondiente a la cláusula SELECT.
#' @param cFrom character: Cadena de texto correspondiente a la cláusula FROM.
#' @param cWhere character: Cadena de texto correspondiente a la cláusula WHERE. Por
#' defecto toma el valor NULL.
#' @param cGroupBy character: Cadena de texto correspondiente a la cláusula GROUP BY. Por
#' defecto toma el valor NULL.
#' @param batch numeric: Tamaño del lote. Por defecto es 1,500.
#' @keywords SQL, RODBC
#' @import RODBC
#' @export mxQueryBatch
#' @return data.frame
#' @examples
#' mxQueryBatch(handle = dbhandle,
#'              vcListaRef = c("001512", "002548"),
#'              cNombreRef = "codigo",
#'              cSelect = "codigo, descripcion",
#'              cFrom = "tabla_general")

mxQueryBatch <- function(handle, vcListaRef, cNombreRef,
                         cSelect, cFrom, cWhere = NULL, cGroupBy = NULL,
                         batch = 1500){
  # Fecha de modificacion: 2018-02-21
  # Responsable de modificacion: Jose Vallejo
  # Descripcion del cambio: Se corrige el cálculo cuando el tamaño del lote es
  # inconsistente con el tamaño del vector de observaciones.
  # Versión de sumision: 0.2

  library(RODBC)
  nRows <- length(vcListaRef)

  if(nRows%/%batch == 0){
    df12 <- sqlQuery(handle,
                     paste0("
                            SELECT ",cSelect,"
                            FROM ",cFrom,"
                            WHERE ",
                            ifelse(is.null(cWhere),"",paste0(cWhere," AND ")),
                            cNombreRef," IN ('", paste(vcListaRef, collapse = "','"), "') ",
                            ifelse(is.null(cGroupBy),"",paste0("GROUP BY ", cGroupBy))),
                     as.is = TRUE)

  } else {
    # Obtiene la fecha de vigencia de los creditos
    df9 <- NULL
    # Efectua la consulta del dividendo
    for(j in 0:as.numeric(nRows%/%batch-1)){
      lista1 <- vcListaRef[(j*batch+1):((j+1)*batch)]
      query1 <- paste0("
                       SELECT ",cSelect,"
                       FROM ",cFrom,"
                       WHERE ",
                       ifelse(is.null(cWhere),"",paste0(cWhere," AND ")),
                       cNombreRef," IN ('", paste(lista1, collapse = "','"), "') ",
                       ifelse(is.null(cGroupBy),"",paste0("GROUP BY ", cGroupBy))
      )

      # Ejecuta la consulta del lote
      df10 <- sqlQuery(handle, query1, as.is = TRUE)

      # Acumula los resultados
      df9 <- rbind(df9,df10)
    }

    # Efectua la consulta del remanente
    lista2 <- vcListaRef[(nRows-nRows%%batch+1):nRows]
    query2 <- paste0("
                     SELECT ",cSelect,"
                     FROM ",cFrom,"
                     WHERE ",
                     ifelse(is.null(cWhere),"",paste0(cWhere," AND ")),
                     cNombreRef," IN ('", paste(lista2, collapse = "','"), "') ",
                     ifelse(is.null(cGroupBy),"",paste0("GROUP BY ", cGroupBy))
    )

    # Ejecuta la consulta del lote
    df11 <- sqlQuery(handle, query2, as.is = TRUE)

    # Acumula los resultados
    df12 <- rbind(df9, df11)
  }
  return(df12)
}
