select kk.chave as id, row_to_json(kk)::text as json  from (
select          p.chave,
                p.dt_geracao,
                p.hh_geracao,
                p.ano_eleicao,
                p.nr_processo,
                p.dt_autuacao,
                p.dt_baixa,
                p.sg_uf_tribunal_origem,
                p.nr_instancia_origem,
                p.sg_uf_tribunal,
                p.nr_instancia,
                p.dt_distribuicao,
                p.cd_tipo_distribuicao,
                p.ds_tipo_distribuicao,
                p.cd_relator,
                p.nm_relator,
                p.cd_tipo_cargo_relator,
                p.ds_tipo_cargo_relator,
                p.cd_classe,
                p.sg_classe,
                p.ds_classe,
                p.cd_assunto_principal,
                p.ds_assunto_principal,
                p.st_concluso,
                p.st_em_pauta,
                p.st_sobrestado,
                p.st_pedido_vista,
                p.st_carga_vista_mpe,
                p.st_recursal,
                p.st_remessa_superior,
                p.qt_decisoes,
                count(pa.nr_processo) conta_partes,
                count(a.nr_processo) conta_assuntos,
                count(d.nr_processo) conta_decisoes,
                json_agg(json_build_object(
                        'cd_assunto', a.cd_assunto,
                        'ds_assunto', a.ds_assunto,
                        'sg_uf_tribunal_origem', a.sg_uf_tribunal_origem)) as assuntos,
                json_agg(json_build_object(
                        'ds_tipo_decisao', d.ds_tipo_decisao,
                        'dt_decisao', d.dt_decisao,
                        'nm_autor_decisao', d.nm_autor_decisao,
                        'sq_decisao', d.sq_decisao)) as decisoes,
                json_agg(json_build_object(
                        'ds_polo', pa.ds_polo,
                        'nm_parte', pa.nm_parte,
                        'nm_social_parte', pa.nm_social_parte,
                        'nr_instancia', pa.nr_instancia,
                        'sg_tribunal_origem', pa.sg_tribunal_origem,
                        'sg_uf_orgao', pa.sg_uf_orgao,
                        'sq_candidato', pa.sq_candidato,
                        'st_candidato', pa.st_candidato,
                        'st_parte_principal', pa.st_parte_principal,
                        'tp_parte', pa.tp_parte)) as partes
from public.processos p
         left join public.assuntos a on (a.nr_processo = p.nr_processo and p.sg_uf_tribunal_origem = a.sg_uf_tribunal_origem)
         left join public.decisoes d on (d.nr_processo = p.nr_processo and p.sg_uf_tribunal_origem = d.sg_uf_tribunal_origem)
         left join public.partes pa on (pa.nr_processo = p.nr_processo and p.sg_uf_tribunal_origem = pa.sg_tribunal_origem and  p.ano_eleicao = pa.ano_eleicao and  p.nr_instancia = pa.nr_instancia)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30 ) kk
where kk.chave in (?);
-- -----------------------------------
--
-- select p.nr_processo,
--        p.ano_eleicao,
--        p.dt_autuacao,
--        p.dt_baixa,
--        json_agg(json_build_object('cd_assunto', a.cd_assunto, 'ds_assunto', a.ds_assunto)) as assuntos
-- from processos p
--          left join public.assuntos a on p.nr_processo = a.nr_processo
-- group by 1, 2, 3, 4;
-- --   -- ,
-- -- array(a.cd_assunto) as cd_assunto
--
-- --
--
-- ;
