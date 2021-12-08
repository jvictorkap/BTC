
-- TAXAS DE ALUGUEL
-- Fonte: UP2Data
--host='PGKPTL01' dbname='Risk_DB' user='kapitalo11' password='kapitalo11'")
SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, 
valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, 
takravrgrate, takrmaxrate, mkt, mktnm, datasts
FROM b3up2data.equities_assetloanfilev2
WHERE rptdt = '2021-03-12'

-- OPEN POSITION
-- Fonte: UP2Data
--host='PGKPTL01' dbname='Risk_DB' user='kapitalo11' password='kapitalo11'")
SELECT rptdt, tckrsymb, NULL AS empresa, NULL as Tipo, isin, balqty, tradavrgpric,pricfctr, balval
FROM b3up2data.equities_securitieslendingpositionfilev2
WHERE rptdt = '2021-03-12'


-- POSIÇÕES CONSOLIDADAS DE BTB
-- Fonte: st_alugcustcorr ==> de onde vem? Imbarq?
SELECT registro, st_alugcustcorr.corretora,st_alugcustcorr.cliente,codigo, vencimento, taxa, 
        avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end 
    FROM st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.cliente=st_alug_devolucao.cliente 
    and st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='2021-03-15' 
    WHERE data='2021-03-12' AND st_alugcustcorr.cliente='KAPITALO KAPPA MASTER FIM'

-- POSIÇÕES DETALHADAS DE BTB
-- Fonte: st_alugcustcorr ==> de onde vem? Imbarq?
SELECT st_alugcustcorr.contrato, registro, corretora, st_alugcustcorr.cliente,codigo, 
    vencimento, taxa, (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end), avg(cotliq) 
    from st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.cliente=st_alug_devolucao.cliente and st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='" & Format(dte_data_liq, "yyyy-mm-dd") & "' 
    where data='2021-03-12' and st_alugcustcorr.cliente<>''  AND st_alugcustcorr.cliente='KAPITALO KAPPA MASTER FIM'
    group by st_alugcustcorr.contrato,registro, corretora,st_alugcustcorr.cliente,codigo, vencimento, taxa  HAVING (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end)<>0  
    order by codigo,vencimento,st_alugcustcorr.cliente,contrato 

-- POSIÇÕES DE AÇÕES A VISTA
-- Fonte: tbl_carteira1
--host='PGKPTL01' dbname='db_Teste' user='kapitalo11' password='kapitalo11'
SELECT str_fundo, str_codigo, regexp_replace(str_serie,' .*',''), sum(dbl_lote) 
    from tbl_carteira1 
    where dte_data='2021-03-12' and str_mercado='Acao' and str_serie<>'DIVIDENDOS' AND str_fundo='KAPITALO KAPPA MASTER FIM' 
    group by str_fundo, str_codigo,str_serie order by str_fundo, str_codigo,str_serie


-- PREÇOS DAS AÇÕES
--db_Kapitalo_v1
SELECT split_part(str_serie,' ',1) , dbl_preco  FROM tbl_mtm  WHERE dte_data = '2021-03-12' AND str_bolsa='BOVESPA' AND str_mercado = 'Acao'

