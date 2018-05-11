#' Base de análisis de cartera
#'
#' Función que extrae las siguientes características de los créditos desembolsados:
#' \itemize{
#'		\item NUM_SOL. Número de solicitud...
#'}
#' @import RODBC
#' @examples base <- mxBase(dbhandle)

mxBase <- function(handle){
  library(RODBC)
  cQueryBase <- "select distinct
  -------------------------------------------INFORMATIVAS----------------------------------------------------
  NUM_solicitud=cre.NUM_SOL,        --número de solicitud de crédtio
  cre.NUM_CRE,                      --número de crédito
  COD_cliente=cre.cod_cli,          --código de cliente
  -------------------------------------------PARA ANÁLISIS---------------------------------------------------
  --NÚM DE IFIS
  num_emp = isnull(tab5.num_emp,0),
  --NIVEL DE APROBACIÓN / TAMAÑO DE DESEMBOLSO
  cod_nivel = (CASE
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end )   <=5000 then '001'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=15000 then '002'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=25000 then '003'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=50000 then '004'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=100000 then '005' else '006'
  END),
  nivel = (CASE
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end )   <=5000 then 'Jefe Créditos'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=15000 then 'Administrador'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=25000 then 'Jefe Regional'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=50000 then 'Gerente de Negocios'
  when (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end ) <=100000 then 'Gerente General' else 'Directorio'
  END),
  --SECTOR ECONÓMICO
  cre.cod_des,
  Sector_economico = eco.des_act_eco,
  --PRODUCTO
  cod_producto = (CASE WHEN his.tip_cre_pr='COMERCIAL' or his.tip_cre_pr='DIARIO' or his.tip_cre_pr='MES' THEN '003'
  WHEN his.tip_cre_pr='CONSUMO' or his.tip_cre_pr='MIVIVIENDA' or his.tip_cre_pr='VIVEMEJOR' THEN '002'
  WHEN his.tip_cre_pr='AGRICOLA' THEN '001'  END),
  desc_producto = (CASE WHEN his.tip_cre_pr='COMERCIAL' or his.tip_cre_pr='DIARIO' or his.tip_cre_pr='MES' THEN 'MYPE'
  WHEN his.tip_cre_pr='CONSUMO' or his.tip_cre_pr='MIVIVIENDA' or his.tip_cre_pr='VIVEMEJOR' THEN 'CONSUMO'
  WHEN his.tip_cre_pr='AGRICOLA' THEN 'AGRICOLA' 	 END),
  --TIPO DE CRÉDITO
  cre.cre_tip_cre_sbs,
  TIPCRE.tip_cre_des,
  --MODALIDAD DE FINANCIAMIENTO
  cre.cre_ind_paral,
  cre.ind_nue_des,
  cre.ind_ref,
  --DESEMBOLSO
  cre.fec_des_cre,      --fecha
  fec_cierre = EOMONTH(cre.fec_des_cre),                 --fecha de cierre del mes
  mon_des = (case when cre.COD_MON_CRE='01' then cre.mon_des else cre.mon_des*(cast(tc2.tca_fij as decimal(12,6) ))  end),          --monto desembolsado
  --SECTORISTA
  IND_cambio_asesor = case when cre.cod_sec<>cre.cod_sec_ori then 1 else 0 end,
  cod_sec_ori = case when ind_ref =1 then tab4.cod_sec else his.cod_sec_ori end,
  tipo = ase.tipo,
  tipo_desc = (CASE WHEN ase.tipo =1  THEN 'ASESOR'
  WHEN ase.tipo =2  THEN 'GESTOR'
  WHEN ase.tipo =3  THEN 'LEGAL'   END),
  Asesor_origen = ase.nom_sec,
  ase.vigente,
  --AGENCIA/O.I.
  cre.cod_ofi_inf,
  Oficina = ofi.des_ofi_lar,
  --MONEDA
  cre.cod_mon_cre,
  Moneda = mon.Nom_Moneda,
  --EVALUACIÓN
  efi.efi_cod_usu_res,  --Usuario responsable
  efi.efi_cod_usu_ev,   --Usuario evaluador
  efi.efi_cod_usu,      --Usuario
  --OTROS
  neg.DIST, --distrito del negocio
  nEdadDesem = year(cre.fec_des_cre) -year(cli.fec_nac), -- edad que tenia el cliente cuando solicitó el crédito
  ciiu2.cod_act_eco, --codigo actividad económica 1
  ciiu2.val_par as val_par1, -- descripción actividad económica 1
  ciiusec2.cod_act_eco_sec, -- código de actividad económica secundaria
  ciiusec2.val_par as val_par2, -- descripción actividad económica secundaria
  --NÚMERO DE EXCEPCIONES
  num_exc=isnull(tab_excepciones.num_exc ,0)
  from
  rie_app_credito cre
  left join rie_app_CREDITO_his his on his.NUM_SOL =cre.NUM_SOL      --oficinas
  left join rie_app_ofi ofi on ofi.cod_ofi =cre.cod_ofi_inf      --oficinas
  left join rie_App_Asesor ase on ase.cod_sec = cre.cod_sec_ori    --asesores
  left join rie_App_SecEco eco on eco.cod_sec_eco = cre.cod_des    --sectores económicos
  left join rie_App_Moneda mon on cre.cod_mon_cre = mon.cod_moneda   --tipo de moneda
  left join rie_App_cre_Efi efi on efi.efi_num_sol= cre.cre_efi_num_sol   --base general
  left join Rie_App_Cre_Tipo tipcre on tipcre.tip_cre_cod = cre.cre_tip_cre_sbs --TIPO DE CRÉDITO
  left join Rie_App_Tip_Cambio tc2 on tc2.fec_cam = cre.fec_des_cre ------TIPO DE CAMBIO
  left join Rie_App_Dir_Neg  neg on neg.num_sol = HIS.NUM_SOL --- distrito del negocio
  left join Rie_App_Cliente cli on cli.cod_cli = his.COD_CLI --- edad del cliente
  left join ( select cod_act_eco, val_par,cli2.COD_CLI from Rie_App_Cliente cli2 left join Rie_App_Ciiu ciiu on ciiu.cod_par = COD_ACT_ECO ) ciiu2 on ciiu2.cod_cli = his.COD_CLI --actividad económica del cliente
  left join ( select cod_act_eco_Sec, val_par,cli2.COD_CLI from Rie_App_Cliente cli2 left join Rie_App_Ciiu ciiusec on ciiusec.cod_par = COD_ACT_ECO_sec ) ciiusec2 on ciiusec2.cod_cli = his.COD_CLI --actividad económica secundaria
  left join (select
  his.cod_sec, cre.NUM_CRE
  from
  rie_app_credito_his his
  inner join
  rie_app_credito cre on cre.num_cre =his.num_cre and month(cre.FEC_DES_cre)= month (his.fec_pro_cla_sbs) and  year(cre.FEC_DES_cre)= year (his.fec_pro_cla_sbs) and ind_ref = 1
  ) tab4 on his.num_cre = tab4.NUM_CRE
  left join
  (SELECT
  count (cre.num_cre) as num_exc,
  cre.num_Cre
  FROM (select distinct
  num_cre,NUM_SOL
  from Rie_App_Credito) cre
  INNER JOIN [Rie_App_Excep_Cred  ] om ON cre.NUM_SOL = om.num_sol
  WHERE  om.tipo='EXCEPCION'
  group by cre.num_cre) tab_excepciones on tab_excepciones.NUM_CRE = cre.NUM_CRE     --número de excepciones
  left join (select distinct
  rcc.FEC_REp,
  num_emp,
  cod_deu_sbs,
  cre.num_cre
  from
  Rie_App_Credito cre
  INNER join Rie_App_Cliente cli on cli.COD_CLI = cre.COD_CLI
  INNER join Rie_App_Saldos_Rcc rcc on cli.COD_SBS_CLI = rcc.cod_deu_sbs
  where year(rcc.fecha_cierre) =year(cre.fec_des_cre)
  and month(rcc.fecha_cierre)  =month(cre.fec_des_cre)
  ) tab5 on  cre.num_cre = tab5.NUM_CRE
  where cre.fec_des_cre>='2015-10-01'
  "
  dfBase <- sqlQuery(channel = handle,
                     query = cQueryBase,
                     as.is = TRUE)

  dfBase$num_emp <- as.numeric(dfBase$num_emp)
  dfBase$cod_nivel <- as.factor(dfBase$cod_nivel)
  dfBase$nivel <- as.factor(dfBase$nivel)
  dfBase$cod_des <- as.factor(dfBase$cod_des)
  dfBase$Sector_economico <- as.factor(dfBase$Sector_economico)
  dfBase$cod_producto <- as.factor(dfBase$cod_producto)
  dfBase$desc_producto <- as.factor(dfBase$desc_producto)
  dfBase$cre_tip_cre_sbs <- as.factor(dfBase$cre_tip_cre_sbs)
  dfBase$tip_cre_des <- as.factor(dfBase$tip_cre_des)
  dfBase$cre_ind_paral <- as.factor(dfBase$cre_ind_paral)
  dfBase$ind_nue_des <- as.factor(dfBase$ind_nue_des)
  dfBase$ind_ref <- as.factor(dfBase$ind_ref)
  dfBase$fec_des_cre <- as.Date(dfBase$fec_des_cre)
  dfBase$fec_cierre <- as.Date(dfBase$fec_cierre)
  dfBase$mon_des <- as.numeric(dfBase$mon_des)
  dfBase$IND_cambio_asesor <- as.factor(dfBase$IND_cambio_asesor)
  dfBase$cod_sec_ori <- as.factor(dfBase$cod_sec_ori)
  dfBase$tipo <- as.factor(dfBase$tipo)
  dfBase$tipo_desc <- as.factor(dfBase$tipo_desc)
  dfBase$Asesor_origen <- as.factor(dfBase$Asesor_origen)
  dfBase$vigente <- as.factor(dfBase$vigente)
  dfBase$cod_ofi_inf <- as.factor(dfBase$cod_ofi_inf)
  dfBase$Oficina <- as.factor(dfBase$Oficina)
  dfBase$cod_mon_cre <- as.factor(dfBase$cod_mon_cre)
  dfBase$Moneda <- as.factor(dfBase$Moneda)
  dfBase$efi_cod_usu_res <- as.factor(dfBase$efi_cod_usu_res)
  dfBase$efi_cod_usu_ev <- as.factor(dfBase$efi_cod_usu_ev)
  dfBase$efi_cod_usu <- as.factor(dfBase$efi_cod_usu)
  dfBase$DIST <- as.factor(dfBase$DIST)
  dfBase$nEdadDesem <- as.numeric(dfBase$nEdadDesem)
  dfBase$cod_act_eco <- as.factor(dfBase$cod_act_eco)
  dfBase$val_par1 <- as.factor(dfBase$val_par1)
  dfBase$cod_act_eco_sec <- as.factor(dfBase$cod_act_eco_sec)
  dfBase$val_par2 <- as.factor(dfBase$val_par2)
  dfBase$num_exc <- as.numeric(dfBase$num_exc)
  return(dfBase)
}