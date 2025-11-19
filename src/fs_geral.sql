-- Defina o parâmetro :dtRef no momento da execução

WITH tb_ativa AS (
  SELECT *
  FROM academy.silver.transacoes
  WHERE DtCriacao - INTERVAL 3 HOUR < :dtRef
    AND DtCriacao - INTERVAL 3 HOUR >= :dtRef - INTERVAL 28 DAY
  ORDER BY DtCriacao DESC
),

-- Calcula a recência (dias desde a última transação) por cliente
tb_recencia AS (
  SELECT IdCliente,
         MIN(DATEDIFF(:dtRef, DtCriacao - INTERVAL 3 HOUR)) AS nrRecencia
  FROM tb_ativa
  GROUP BY IdCliente
),

-- Calcula saldo de pontos e idade da base por cliente
tb_vida AS (
  SELECT IdCliente,
         SUM(QtdePontos) AS nrSaldoPontos,
         MAX(DATEDIFF(:dtRef, DtCriacao - INTERVAL 3 HOUR)) AS nrIdadeBase
  FROM academy.silver.transacoes
  WHERE DtCriacao - INTERVAL 3 HOUR < :dtRef
    AND IdCliente IN (SELECT IdCliente FROM tb_recencia)
  GROUP BY IdCliente
),

-- Junta as informações de recência, saldo de pontos, idade da base e flag de e-mail
tb_final AS (
  SELECT t1.*,
         t2.nrSaldoPontos,
         t2.nrIdadeBase,
         t3.flEmail AS flEmail
  FROM tb_recencia AS t1
  LEFT JOIN tb_vida AS t2
    ON t1.idCliente = t2.idCliente
  LEFT JOIN academy.silver.clientes AS t3
    ON t1.idCliente = t3.idCliente
)

-- Seleciona resultado final com data de referência
SELECT :dtRef AS dtRef,
       *
FROM tb_final;