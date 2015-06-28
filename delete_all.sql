delete from obra_municipio
WHERE id_obra>0 OR id_obra = 0;

delete from obra_executor
WHERE id_obra>0 OR id_obra = 0;

delete from municipio
WHERE id_municipio>0 OR id_municipio = 0;

delete from estado
WHERE id_estado>0 OR id_estado = 0;

delete from executor
WHERE id_executor>0 OR id_executor = 0;

DELETE FROM obra
WHERE id_obra>0 OR id_obra = 0;

DELETE FROM orgao
WHERE id_orgao>0 OR id_orgao = 0;