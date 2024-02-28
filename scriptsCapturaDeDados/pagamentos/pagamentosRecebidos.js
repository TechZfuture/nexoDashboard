const mysql = require("mysql2/promise");
const moment = require("moment");

const dbConfig = require("../../configuracoesBancoDeDados/configBancoDeDados");
const apitoken = require("../../../apitokens/configuracoesNexo/configAPI");

// FUNÇÃO PARA RETORNAR O ANO E O MÊS ATUAL
function anoMesAtual() {
  const dataAtual = new Date();
  const ano = dataAtual.getFullYear();
  const mes = dataAtual.getMonth() + 1;
  return { ano, mes };
}
const { ano, mes } = anoMesAtual();

// FUNÇÃO RESPONSÁVEL POR CAPTURAR OS DADOS DO NIBO
async function buscarDados() {
  // const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/credit/opened?apitoken=${apitoken}`;
  const apiUrl = `https://api.nibo.com.br/empresas/v1/schedules/credit/opened?$filter=year(accrualDate) eq ${ano} and month(accrualDate) eq ${mes}&apitoken=${apitoken}`;

  try {
    const data = await (await fetch(apiUrl)).json();
    return data.items || [];
  } catch (error) {
    console.error("Erro ao buscar dados da API:", error);
    return [];
  }
}

// FORMATAR DATA PARA O MYSQL
function formatarDataParaMySQL(data) {
  return moment(data).format("YYYY-MM-DD HH:mm:ss");
}

// FUNÇÃO RESPONSÁVEL POR ARMAZENAR OS DADOS NO BANCO DE DADOS
async function inserirDados(data) {
  const connection = await mysql.createConnection(dbConfig);
  let [atualizadas, inseridas] = [0, 0];

  try {
    for (const item of data) {
      const [existe] = await connection.execute(
        `SELECT * FROM paymentsReceivable id WHERE id = ?`,
        [item.categories[0].id]
      );

      const query =
        existe.length > 0
          ? `UPDATE paymentsReceivable SET categoryId = ?, categoryName = ?, value = ?, type = ?, parent = ?, parentId = ?, scheduleId = ?, typeOperation = ?, isEntry = ?, isBill = ?,
            isDebiteNote = ?, isFlagged = ?, isDued = ?, dueDate = ?, accrualDate = ?, scheduleDate = ?, createDate = ?, isPaid = ?, costCenterValueType = ?, paidValue = ?,
            openValue = ?, stakeholderId = ?, stakeholderType = ?, stakeholderName = ?, stakeholderIsDeleted = ?, description = ?, reference = ?, hasInstallment = ?, installmentId = ?,
            hasRecurrence = ?, hasOpenEntryPromise = ?, hasEntryPromise = ?, autoGenerateEntryPromise = ?, hasInvoice = ?, hasPendingInvoice = ?, hasScheduleInvoice = ?,
            autoGenerateNfseType = ?, isPaymentScheduled = ? WHERE id = ?`
          : `INSERT INTO paymentsReceivable (id, categoryId, categoryName, value, type, parent, parentId, scheduleId, typeOperation, isEntry, isBill, isDebiteNote, isFlagged,
              isDued, dueDate, accrualDate, scheduleDate, createDate, isPaid, costCenterValueType, paidValue, openValue, stakeholderId, stakeholderType, stakeholderName,
              stakeholderIsDeleted, description, reference, hasInstallment, installmentId, hasRecurrence, hasOpenEntryPromise, hasEntryPromise, autoGenerateEntryPromise,
              hasInvoice, hasPendingInvoice, hasScheduleInvoice, autoGenerateNfseType, isPaymentScheduled, status, valorCorreto) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      const params =
        existe.length > 0
          ? [
              item.category.id || null,
              item.category.name || null,
              item.value || null,
              item.category.type || null,
              item.categories[0].parent || null,
              item.categories[0].parentId || null,
              item.scheduleId || null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.isDebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              formatarDataParaMySQL(item.createDate) || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.id || null,
              item.stakeholder.type || null,
              item.stakeholder.name || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.reference || null,
              item.hasInstallment || null,
              item.installmentId || null,
              item.hasRecurrence || null,
              item.hasOpenEntryPromise || null,
              item.hasEntryPromise || null,
              item.autoGenerateEntryPromise || null,
              item.hasInvoice || null,
              item.hasPendingInvoice || null,
              item.hasScheduleInvoice || null,
              item.autoGenerateNFSeType || null,
              item.isPaymentScheduled || null,
              item.scheduledId || null,
            ]
          : [
              item.categories[0].id || null,
              item.categories[0].categoryId || null,
              item.categories[0].categoryName || null,
              item.categories[0].value || null,
              item.categories[0].type || null,
              item.categories[0].parent || null,
              item.categories[0].parentId || null,
              item.scheduleId || null,
              item.type || null,
              item.isEntry || null,
              item.isBill || null,
              item.DebitNote || null,
              item.isFlagged || null,
              item.isDued || null,
              formatarDataParaMySQL(item.dueDate) || null,
              formatarDataParaMySQL(item.accrualDate) || null,
              formatarDataParaMySQL(item.scheduleDate) || null,
              formatarDataParaMySQL(item.createDate) || null,
              item.isPaid || null,
              item.costCenterValueType || null,
              item.paidValue || null,
              item.openValue || null,
              item.stakeholder.id || null,
              item.stakeholder.type || null,
              item.stakeholder.name || null,
              item.stakeholder.isDeleted || null,
              item.description || null,
              item.reference || null,
              item.hasInstallment || null,
              item.installmentId || null,
              item.hasRecurrence || null,
              item.hasOpenEntryPromise || null,
              item.hasEntryPromise || null,
              item.autoGenerateEntryPromise || null,
              item.hasInvoice || null,
              item.hasPendingInvoice || null,
              item.hasScheduleInvoice || null,
              item.autoGenerateNFSeType || null,
              item.isPaymentScheduled || null,
              null,
              null,
            ];

      const [result] = await connection.execute(query, params);

      existe.length > 0
        ? (atualizadas += result.changedRows)
        : (inseridas += result.affectedRows);
    }
    console.log("\n-- Pagamentos Recebidos --");
    console.log(
      `${atualizadas} consultas atualizadas no banco de dados.\n${inseridas} consultas inseridas no banco de dados.`
    );
  } catch (error) {
    console.error("Erro ao inserir dados no banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

// FUNÇÃO RESPONSÁVEL POR VERIFICAR SE O DADO QUE EXISTE NO BANCO CONDIZ COM O DADO RETORNADO DA API, CASO NÃO, ELE É DELETADO DO BANCO DE DADOS
async function deletarDados(data) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const idsNoBanco = new Set(data.map((item) => item.scheduleId));
    const registrosNoBanco = (
      await connection.execute("SELECT scheduleId FROM paymentsReceivable")
    )[0];

    for (const { scheduleId } of registrosNoBanco) {
      if (!idsNoBanco.has(scheduleId)) {
        await connection.execute(
          "DELETE FROM paymentsReceivable WHERE scheduleId = ?",
          [scheduleId]
        );
      }
    }
  } catch (error) {
    console.error("Erro ao deletar dados no banco de dados:", error);
  } finally {
    connection.end(); // Feche a conexão com o banco de dados
  }
}

// INSERIR DADO NA COLUNA STATUS
async function inserirColunaStatus() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE paymentsReceivable
        SET status = "A Realizar"
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

// INSERIR O VALOR CORRETO
async function inserirValorCorreto() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Atualiza o status com base na diferença entre openValue e paidValue
    const query = `
        UPDATE paymentsReceivable
        SET valorCorreto = CASE
        WHEN paidValue IS NOT NULL THEN value - paidValue
        ELSE value
        END;
      `;
    await connection.execute(query);
  } catch (error) {
    console.error('Erro ao inserir valor na coluna "status":', error);
  } finally {
    connection.end(); // Fecha a conexão com o banco de dados
  }
}

// FUNÇÃO PRINCIPAL
(async () => {
  try {
    const dadosDaAPI = await buscarDados();
    if (dadosDaAPI.length > 0) {
      await inserirDados(dadosDaAPI);
      // await deletarDados(dadosDaAPI);
      await inserirColunaStatus();
      await inserirValorCorreto();
    }
  } catch (error) {
    console.error("Erro no processo:", error);
  }
})();
