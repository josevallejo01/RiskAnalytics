#' Evolución de la mora ajustada por cosechas
#'
#' Construye el cuadro de evolución mora ajustada por cosechas dado un periodo de
#' análisis. La función incluye la posibilidad de agrupar los meses de
#' desembolso en periodos de más de un mes con el uso del parámetro "periodo".
#' Se asume que el último periodo de análisis disponible es un mes anterior a la fecha
#' en que se hace la consulta.
#' Además del marco de datos con información de cosechas, se generan los data.frame "dfMontoDesem"
#' y "dfNumCreDesem" con el detalle de desembolsos y número de créditos desembolsados
#' manteniendo la misma estructura.
#' @param handle integer: Objeto de clase RODBC que establece la conexión al servidor.
#' @param vNumCre vector: Conjunto de códigos de crédito para el cálculo de la mora ajustada por cosechas.
#' @param cFecIni character: Fecha de inicio del periodo total de análisis.
#' @param cFecFin character: Fecha de fin del periodo total de análisis.
#' @param periodo numeric: Número de meses de agrupación para formar grupos temporales. Por defecto es 1.
#' @param batch numeric: Tamaño del lote. Por defecto es 1,500.
#' @param bExportTab logical: Indica si el marco de datos resultante debe ser exportado en un fichero.
#' @param cNomTab character: En caso se exporte el marco de datos, nombre del fichero.
#' @keywords SQL, RODBC, Cosechas
#' @import dplyr, lubridate, reshape2
#' @export mxMoraCosecha
#' @return data.frame
#' @examples
#' mora <- mxMoraCosecha(handle = dbhandle,
#'                       vNumCre = c("001478000088","101478000088"),
#'                       cFecIni = "2017-01-01",
#'                       cFecFin = "2017-12-31",
#'                       periodo = 3)

mxMoraCosecha <- function(handle, vNumCre, cFecIni, cFecFin,  periodo, batch = 1500,
                          bExportTab = FALSE, cNomTab){

  tab_gen <- mxQueryBatch(handle = handle,vcListaRef = vNumCre,
                          cNombreRef = "mhis.num_cre",
                          cSelect = "mhis.num_cre, mhis.fec_cierre,mhis.fec_des_cre,
                          mon_des= (round((case when mhis.COD_MON_CRE='01' then mhis.mon_eje else mhis.mon_EJE*( cast( tc1.tca_fij as decimal(12,6) ) ) end) ,0) )
                          ,(case when mhis.COD_MON_CRE='01' then mhis.saldo_vencido else mhis.saldo_vencido*( cast( tc.tca_fij as decimal(12,6)) ) end) as saldo_vencido
                          ,(case when cre.COD_MON_CRE='01' then cre.mon_cap_cas else cre.mon_cap_cas*( cast( tc2.tca_fij as decimal(12,6)) ) end) as mon_cap_cas
                          ",
                          cFrom = " (select distinct num_cre,fec_cierre,fec_des_cre,fec_can,Estado_cred,ind_cas,ind_nue,ind_ref,RCS,RSS,ind_paral,cod_mon_cre,mon_eje,fec_eje,saldo_vencido from Rie_App_Mora_cosecha_his where (fec_can is null or fec_can='01/01/1900'or ((YEAR(fec_des_cre)<>YEAR(fec_CAN)) and (MONTH(fec_des_cre)<> MONTH(fec_CAN))))) mhis
                          left join  Rie_App_Credito cre on cre.NUM_CRE= mhis.num_cre and mhis.ind_cas=1 and mhis.fec_cierre>=cre.fec_cas
                          left join Rie_App_Tip_Cambio tc on mhis.fec_cierre = tc.fec_cam
                          left join Rie_App_Tip_Cambio tc1 on mhis.fec_des_cre= tc1.fec_cam
                          left join Rie_App_Tip_Cambio tc2 on cre.fec_cas= tc2.fec_cam
                          ",
                          batch = batch)

  library(dplyr)
  library(lubridate)
  library(reshape2)

  tab_gen <- tab_gen %>%
    mutate(fec_des_cre = as.Date(fec_des_cre),
           fec_cierre = as.Date(fec_cierre),
           mon_des = as.numeric(mon_des),
           saldo_vencido = as.numeric(ifelse(is.na(saldo_vencido),0,saldo_vencido)),
           mon_cap_cas = as.numeric(ifelse(is.na(mon_cap_cas),0,mon_cap_cas)),
           nYYYYMMDes = paste0(format(fec_des_cre, "%Y"),
                               format(fec_des_cre, "%m")), # Formato fecha desembolso %Y%m
           nSalVenCas = saldo_vencido + mon_cap_cas, # Calcula mora real
           nMesCosecha = interval(fec_des_cre,fec_cierre)%/%months(1) # Calcula el mes de cosecha
    ) %>%
    filter(fec_des_cre >= cFecIni, fec_des_cre <= cFecFin ) %>% # Solo toma el periodo de analisis
    arrange(num_cre,fec_cierre)

  # Crea tabla consolidadada mensual de desembolsos
  desem_mes <- tab_gen %>%
    select(num_cre, nYYYYMMDes, mon_des) %>%
    distinct() %>%
    group_by(nYYYYMMDes) %>%
    summarise(nMonDes = sum(mon_des), nNumCre = n_distinct(num_cre))


  datseq <- format(seq(as.Date(cFecIni),
                       as.Date(cFecFin),by="month"),
                   "%Y%m")

  # Busca el mes en que no hubo registro de desembolso
  meses_ausentes <- datseq[!(datseq %in% desem_mes$nYYYYMMDes)]

  if(!(identical(character(0), meses_ausentes))){
    desem_mes <- bind_rows(desem_mes,
                       data.frame(nYYYYMMDes = meses_ausentes, nMonDes = 0)
    )

    desem_mes <- desem_mes %>% arrange(nYYYYMMDes)
  }

  # Reemplaza los valores NA por 0 en la tabla de desembolsos
  desem_mes[is.na(desem_mes)] <- 0

  # Crea tabla consolidada mensual de mora ajustada
  cosecha_aj_mes <- tab_gen %>%
    distinct() %>% # Elimina duplicados
    group_by(nYYYYMMDes, nMesCosecha) %>%
    summarise(nSalVenCas = sum(nSalVenCas))

  #Une con tabla de desembolsos
  cosecha_aj_mes <- cosecha_aj_mes %>%
    inner_join(x = cosecha_aj_mes, y = desem_mes, by = "nYYYYMMDes")

  cosecha_aj_mes <- cosecha_aj_mes %>%
    mutate(nMoraCosecha = nSalVenCas/nMonDes)

  cosecha_aj_mes <- dcast(data = cosecha_aj_mes,
                          formula = nYYYYMMDes ~ nMesCosecha,
                          value.var = "nMoraCosecha")

  if(!(identical(character(0), meses_ausentes))){
    # Busca el mes en que no hubo registro de desembolso y completa cosecha_aj_mes
    cosecha_aj_mes <- bind_rows(data.frame(nYYYYMMDes = meses_ausentes), cosecha_aj_mes) %>%
      arrange(nYYYYMMDes)
  }

  temp <- cosecha_aj_mes

  cosecha_aj_mes <- t(as.matrix(cosecha_aj_mes[,2:dim(cosecha_aj_mes)[2]]))

  # Tratamiento cuando el periodo es mayor a 1
  if(periodo > 1){
    # Comprueba si existen segmentos insuficientes
    if((dim(cosecha_aj_mes)[2]/periodo) < 1){ return(NA) }
    else{

      # Crea matriz de cosechas
      # Numero de filas: Numero de meses entre fecha inicial y ultimo cierre disponible. A esto
      # se le quita el periodo para tener numero de meses efectivos y se agrega el mes 0.
      # Numero de columnas: Division entre numero de meses y cantidad de segmentos

      cosecha_aj <- matrix(data = ,
                           nrow = interval(start = as.Date(cFecIni),
                                           end = floor_date(Sys.Date(), "month"))%/%months(1)-periodo+1,
                           ncol = (dim(cosecha_aj_mes)[2]/periodo)
      )

      # Crea tabla de desembolsos por segmento
      vMontoDesem <- vector()

      # Crea tabla de número de créditos por segmento
      vNumCreDesem <- vector()

      for(j in 1:(dim(cosecha_aj_mes)[2]/periodo)){
        vMontoDesem[j] <- sum(desem_mes$nMonDes[(periodo*j-periodo+1):(periodo*j)])
        vNumCreDesem[j] <- sum(desem_mes$nNumCre[(periodo*j-periodo+1):(periodo*j)])
      }

      # Multiplicación cruzada de matrices para ponderar mora ajustada en agrupaciones periodicas
      for(i in 1:(dim(cosecha_aj_mes)[1]- periodo + 1)){
        for(j in 1:(dim(cosecha_aj_mes)[2]/periodo)){
          if(
            # Controla que los calculos se enmarquen en lo posible
            i > (dim(cosecha_aj)[1] + periodo - periodo*(j)) ||
            # Si en uno de los meses que conforma el segmento no hubo desembolsos, no se debe hacer el cálculo
            0 %in% desem_mes$nMonDes[(periodo*j-periodo+1):(periodo*j)]
            ){cosecha_aj[i,j] <- NA }
          else{
            # Se reemplazan NA con 0 para no perder informacion de mora cosecha
            cosecha_aj[i,j] <- as.numeric(crossprod(replace(cosecha_aj_mes[i,(periodo*j-periodo+1):(periodo*j)],
                                                            is.na(cosecha_aj_mes[i,(periodo*j-periodo+1):(periodo*j)]),0),
                                                    desem_mes$nMonDes[(periodo*j-periodo+1):(periodo*j)])/sum(desem_mes$nMonDes[(periodo*j-periodo+1):(periodo*j)]))

          }
        }
      }
    }

    cosecha_aj <- data.frame(cosecha_aj, stringsAsFactors = FALSE)
    dfMontoDesem <- data.frame(t(vMontoDesem), stringsAsFactors = FALSE)
    dfNumCreDesem <- data.frame(t(vNumCreDesem), stringsAsFactors = FALSE)

    # Da nombre a las columnas
    for(i in 1:(length(temp$nYYYYMMDes)%/%periodo)){
      colnames(cosecha_aj)[i] <- paste(temp$nYYYYMMDes[periodo*i-periodo+1],
                                       temp$nYYYYMMDes[i*periodo],
                                       sep="_")
    }


    colnames(dfMontoDesem) <- colnames(cosecha_aj)
    row.names(dfMontoDesem) <- "nMonDes"

    colnames(dfNumCreDesem) <- colnames(cosecha_aj)
    row.names(dfNumCreDesem) <- "nNumCreDes"

    # Quita el mes 0 de desembolso
    cosecha_aj[1,] <- NA

    if (ncol(cosecha_aj) == 1) {
      # para ncol == 1 se usa este metodo ya que conserva la estructura df
      cosecha_aj <- na.omit(cosecha_aj)
    } else {
      cosecha_aj <- cosecha_aj[apply(cosecha_aj,1,function(x)any(!is.na(x))),]

      # Selecciona solo filas o columnas que tengan por lo menos un elemento no NA
      keep.cols = which(apply(!is.na(cosecha_aj), 2, any))
      keep.rows = which(apply(!is.na(cosecha_aj), 1, any))

      cosecha_aj = cosecha_aj[keep.rows,keep.cols]
    }

    temp2 <- data.frame(paste0("",seq(1:dim(cosecha_aj)[1])), stringsAsFactors = FALSE)
    cosecha_aj <- cbind(temp2, cosecha_aj)

    colnames(cosecha_aj)[1] <- c("MES")
  }

  # Realiza ajustes adicionales cuando el periodo es mensual
  if(periodo == 1){
    # Ajuste sobre tabla de cosechas
    cosecha_aj = as.data.frame(cosecha_aj_mes[2:dim(cosecha_aj_mes)[1],])
    names(cosecha_aj) <- temp$nYYYYMMDes
    cosecha_aj <- cbind(data.frame(MES = c(1:nrow(cosecha_aj))),cosecha_aj)

    # Ajuste sobre tabla de monto desembolsado
    dfMontoDesem <- data.frame(t(desem_mes$nMonDes), stringsAsFactors = FALSE)
    colnames(dfMontoDesem) <- colnames(cosecha_aj)[-1]
    row.names(dfMontoDesem) <- "nMonDes"

    # Ajuste sobre tabla de número de créditos desembolsados
    dfNumCreDesem <- data.frame(t(desem_mes$nNumCre), stringsAsFactors = FALSE)
    colnames(dfNumCreDesem) <- colnames(cosecha_aj)[-1]
    row.names(dfNumCreDesem) <- "nNumCreDesem"
  }

  if(bExportTab == TRUE){
    library(xtable)

    table <- xtable(x = cosecha_aj,
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

  assign(x = 'dfMontoDesem',
         value = dfMontoDesem,
         envir=.GlobalEnv)

  assign(x = 'dfNumCreDesem',
         value = dfNumCreDesem,
         envir=.GlobalEnv)

  return(cosecha_aj)
}
