const mysql = require("mysql2/promise");
const moment = require("moment");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../../informacoesAPI/nexo");

async function buscarDados() {
    const apiUrl = `https://api.nibo.com.br/empresas/v1/accounts/views/balance?apitoken=${apitoken}`;
  
    try {
      const data = await (await fetch(apiUrl)).json();
      return data.items || [];
    } catch (error) {
      console.error("Erro ao buscar dados da API:", error);
      return [];
    }
  }

  async function inserirDados(data) {
    const connection = await mysql.createConnection(dbConfig);
    let [atualizadas, inseridas] = [0, 0];
  
    try {
      for (const item of data) {
        const [existe] = await connection.execute(
          "SELECT * FROM balance WHERE accountId = ?",
          [item.accountId]
        );
  
        const query =
          existe.length > 0
            ? `UPDATE balance SET accountName = ?, balance = ?, agency = ?, accountNumber = ?,
              isVirtual = ?, isReconcilable = ?, isPJBankVirtualAccountWaitingApprove = ? WHERE accountId = ?`
            : `INSERT INTO balance (accountId, accountName, balance, agency, accountNumber, isVirtual,
                  isReconcilable, isPJBankVirtualAccountWaitingApprove) VALUES (?, ?, ?, ?, ?, ?, ?, ?) `;
  
        const params =
          existe.length > 0
            ? [
                item.accountName || null,
                item.balance || null,
                item.agency || null,
                item.accountNumber || null,
                item.isVirtual || null,
                item.isReconcilable || null,
                item.isPJBankVirtualAccountWaitingApprove || null,
                item.accountId || null,
              ]
            : [
                item.accountId || null,
                item.accountName || null,
                item.balance || null,
                item.agency || null,
                item.accountNumber || null,
                item.isVirtual || null,
                item.isReconcilable || null,
                item.isPJBankVirtualAccountWaitingApprove || null,
              ];
  
        const [result] = await connection.execute(query, params);
  
        existe.length > 0
          ? (atualizadas += result.changedRows)
          : (inseridas += result.affectedRows);
      }
      console.log("\n-- Saldo --")
      console.log(
        `${atualizadas} - consultas atualizadas no banco de dados.\n${inseridas} - consultas inseridas no banco de dados.`
      );
    } catch (error) {
      console.error("Erro ao inserir dados no banco de dados:", error);
    } finally {
      connection.end(); // Feche a conexão com o banco de dados
    }
  }

  async function deletarDados(data) {
    const connection = await mysql.createConnection(dbConfig);
  
    try {
      const idsNoBanco = new Set(data.map((item) => item.accountId));
      const registrosNoBanco = (
        await connection.execute("SELECT accountId FROM balance")
      )[0];
  
      for (const { accountId } of registrosNoBanco) {
        if (!idsNoBanco.has(accountId)) {
          await connection.execute("DELETE FROM balance WHERE accountId = ?", [id]);
          console.log(`Registro com id ${accountId.id} foi excluído.`);
        }
      }
    } catch (error) {
      console.error("Erro ao deletar dados no banco de dados:", error);
    } finally {
      connection.end(); // Feche a conexão com o banco de dados
    }
  }

  (async () => {
    try {
      const dadosDaAPI = await buscarDados();
      if (dadosDaAPI.length > 0) {
        await inserirDados(dadosDaAPI);
        await deletarDados(dadosDaAPI);
      }
    } catch (error) {
      console.error("Erro no processo:", error);
    }
  })();