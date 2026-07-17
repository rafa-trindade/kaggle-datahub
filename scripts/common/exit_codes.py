"""
Códigos de saída padronizados para scripts de extract/process/load.

  SUCESSO       (0) -- rodou certo E há dado novo
  SEM_NOVIDADE  (2) -- rodou certo, mas nada mudou desde a última vez
  ERRO          (1) -- falhou de verdade
"""
SUCESSO = 0
ERRO = 1
SEM_NOVIDADE = 2