
using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    private static readonly SemaphoreSlim semaphore = new SemaphoreSlim(5); // Limita a 5 tareas paralelas
    private const int MAX_RETRIES = 3; // Número máximo de reintentos
    private const int TIMEOUT_SECONDS = 120; // Timeout para las operaciones críticas

    // Método principal
    static async Task Main(string[] args)
    {
        bool repetirProceso = true;

        //Ruta para almacenar LOGS
        string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
        string logsDirectory = Path.Combine(baseDirectory, "LOGS");

        // Crear la carpeta LOGS si no existe
        if (!Directory.Exists(logsDirectory))
        {
            Directory.CreateDirectory(logsDirectory);
        }

        while (repetirProceso)
        {
            // Iniciar el temporizador junto con el menú
            Task<int> menuTask = Task.Run(() = > MostrarMenuConTemporizador()); // Inicia el menú con temporizador
            int opcion = await menuTask; // Espera la opción seleccionada o la automática

            // Variables comunes
            DateTime inicioProcesoGlobal = DateTime.Now;
            List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefsProcesados = new List<(string, string, string, string, string)>();
            List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();
            string fechaInicio, fechaFin, fechaActual = DateTime.Now.ToString("yyyyMMdd");
            TimeSpan tiempoTotalCefs = TimeSpan.Zero; // Para el tiempo total de procesamiento

            // Verificar si se ejecutó la opción automática tras los 20 segundos
            if (opcion == 0)
            {
                // Calcular las fechas automáticas
                fechaInicio = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
                fechaFin = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");

                Console.WriteLine("No hubo interacción. Procesando automáticamente...");
                Console.WriteLine($"Fecha Inicio: {fechaInicio}, Fecha Fin: {fechaFin}, Procesando TODOS los CEFs.");
            }
            else
            {
                // Si el usuario selecciona una opción manualmente
                fechaInicio = ObtenerFecha("inicio");
                fechaFin = ObtenerFecha("fin");
            }

            // Abrir el log para registros

            //string logFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_cargaCEF_{fechaActual}.txt";
            string logFilePath = Path.Combine(logsDirectory, $"log_cargaCEF_{fechaActual}.txt");

            using (StreamWriter log = new StreamWriter(logFilePath, true))
            {
                // Opción 1: Realizar proceso de carga de CEFs completo o automático
                if (opcion == 1 || opcion == 0)
                {
                    List<string> cefsList = opcion == 0 ? new List<string>{ "TODOS" } : ObtenerListaCefs();

                    log.WriteLine("*****************************");
                    log.WriteLine("Inicio del Proceso de Carga CEF");

                    using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
                    {
                        sqlConnection.Open();

                        // Procesar múltiples CEFs
                        if (cefsList.Count == 1 && cefsList[0].ToUpper() == "TODOS")
                        {
                            // Procesar todos los CEFs
                            List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = ObtenerTodosLosDatosConexion();
                            listaCefsProcesados = listaCefs;

                            foreach(var cefDatos in listaCefs)
                            {
                                // Verificar si hay registros existentes para eliminar
                                bool existenRegistros = await VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cefDatos.sigla, log);
                                if (existenRegistros)
                                {
                                    await EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cefDatos.sigla);
                                }
                            }

                            if (listaCefs.Count == 0)
                            {
                                log.WriteLine("No se encontraron CEFs activos para procesar.");
                            }
                            else
                            {
                                // Obtener tiempo total de procesamiento de todos los CEFs
                                tiempoTotalCefs = await ProcesarCefs(listaCefs, fechaInicio, fechaFin, log, detallesCefs);
                            }
                        }
                        else
                        {
                            foreach(string cef in cefsList)
                            {
                                // Obtener datos de conexión para cada CEF
                                var(ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);

                                if (string.IsNullOrEmpty(ipserver))
                                {
                                    log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}'.");
                                    continue;
                                }

                                // Verificar si hay registros existentes para eliminar
                                bool existenRegistros = await VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cef, log);
                                if (existenRegistros)
                                {
                                    log.WriteLine($"Registro: Registros existentes para el CEF: {cef} en el rango de fechas: {fechaInicio} a {fechaFin}. Se eliminarán los registros.");
                                    await EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cef);
                                }

                                listaCefsProcesados.Add((cef, ipserver, rutadb, userdb, passdb));
                            }

                            // Obtener tiempo total de procesamiento de los CEFs seleccionados
                            tiempoTotalCefs = await ProcesarCefs(listaCefsProcesados, fechaInicio, fechaFin, log, detallesCefs);
                        }
                    }

                    // Resumen global al final del proceso
                    DateTime finProcesoGlobal = DateTime.Now;
                    TimeSpan duracionProcesoGlobal = finProcesoGlobal - inicioProcesoGlobal;
                    string tiempoFormateadoGlobal = $"{duracionProcesoGlobal.Hours} horas, {duracionProcesoGlobal.Minutes} minutos, {duracionProcesoGlobal.Seconds} segundos";
                    string tiempoFormateadoCefs = $"{tiempoTotalCefs.Hours} horas, {tiempoTotalCefs.Minutes} minutos, {tiempoTotalCefs.Seconds} segundos";

                    log.WriteLine("#############################");
                    log.WriteLine($"Inicio del procesamiento: {inicioProcesoGlobal:yyyy-MM-dd HH:mm:ss}");
                    log.WriteLine($"Fin del procesamiento total: {finProcesoGlobal:yyyy-MM-dd HH:mm:ss}");
                    log.WriteLine($"Tiempo total del procesamiento global: {tiempoFormateadoGlobal}");
                    log.WriteLine($"Tiempo total acumulado de procesamiento de CEFs: {tiempoFormateadoCefs}");
                    log.WriteLine("#############################");

                    // Verificación de conteo por CEF
                    Console.WriteLine("Proceso completado, ahora se procede con hacer un conteo por cada CEF.");
                    string logVerificationFilePath = Path.Combine(logsDirectory, $"log_verificacion_ticket_{fechaActual}.txt");
                    //string logVerificationFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_verificacion_ticket_{fechaActual}.txt";
                    await VerificarConteoRegistrosPorCEF(listaCefsProcesados, fechaInicio, fechaFin, logVerificationFilePath);
                }
                // Opción 2: Realizar solo el conteo de registros por CEF
                else if (opcion == 2)
                {
                    List<string> cefsList = ObtenerListaCefs();
                    Console.WriteLine("Verificación de Conteo de Registros por CEF:");

                    string logVerificationFilePath = Path.Combine(logsDirectory, $"log_verificacion_ticket_{fechaActual}.txt");
                    //string logVerificationFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_verificacion_ticket_{fechaActual}.txt";

                    if (cefsList.Count == 1 && cefsList[0].ToUpper() == "TODOS")
                    {
                        // Procesar todos los CEFs
                        listaCefsProcesados = ObtenerTodosLosDatosConexion();
                    }
                    else
                    {
                        foreach(string cef in cefsList)
                        {
                            var(ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);
                            if (!string.IsNullOrEmpty(ipserver))
                            {
                                listaCefsProcesados.Add((cef, ipserver, rutadb, userdb, passdb));
                            }
                        }
                    }

                    await VerificarConteoRegistrosPorCEF(listaCefsProcesados, fechaInicio, fechaFin, logVerificationFilePath);
                }

                // Opción 3: Validar registros entre FA_POS y Cointech
                else if (opcion == 3)
                {
                    await ValidarRegistrosCEF(fechaInicio, fechaFin);
                }
            }

            // Validar y reprocesar errores si los hubo
            await ValidarErrorCef(fechaInicio, fechaFin, opcion);

            repetirProceso = PreguntarRepetirProceso();
        }
    }



    // Método para validar y reprocesar errores desde log_carga_cef
    static async Task ValidarErrorCef(string fechaInicio, string fechaFin, int opcion)
    {
        // Abrir el log para registros
        string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
        string logsDirectory = Path.Combine(baseDirectory, "LOGS");

        // Crear la carpeta LOGS si no existe
        if (!Directory.Exists(logsDirectory))
        {
            Directory.CreateDirectory(logsDirectory);
        }

        string fechaActual = DateTime.Now.ToString("yyyyMMdd");
        string logFilePath = Path.Combine(logsDirectory, $"validacion_error_carga_cef_{fechaActual}.txt");

        using (StreamWriter log = new StreamWriter(logFilePath, true))
        {
            log.WriteLine("*****************************");
            log.WriteLine($"Iniciando validación de errores pendientes en log_carga_cef. Fecha: {DateTime.Now}");

            using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
            {
                await sqlConnection.OpenAsync();

                // Modificamos la consulta para obtener las fechas por separado
                string query = "SELECT CEF, fechaInicio, fechaFin FROM log_carga_cef WHERE estado = 'Fallido'";
                using (var command = new SqlCommand(query, sqlConnection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        List<(string cef, string fechaInicio, string fechaFin)> cefsPendientes = new List<(string cef, string fechaInicio, string fechaFin)>();

                        while (reader.Read())
                        {
                            string cef = reader["CEF"].ToString();
                            string fechaInicioPendiente = reader["fechaInicio"].ToString();
                            string fechaFinPendiente = reader["fechaFin"].ToString();

                            cefsPendientes.Add((cef, fechaInicioPendiente, fechaFinPendiente));
                        }

                        if (cefsPendientes.Count == 0)
                        {
                            log.WriteLine("No se encontraron registros pendientes en log_carga_cef.");
                        }

                        foreach(var(cef, fechaInicioPendiente, fechaFinPendiente) in cefsPendientes)
                        {
                            log.WriteLine($"Validando CEF: {cef} con rango de fechas {fechaInicioPendiente} - {fechaFinPendiente}");
                            bool exito = false;
                            string errorMsg = string.Empty;

                            try
                            {
                                // Intentar reprocesar de acuerdo a la opción seleccionada (carga completa o conteo)
                                if (opcion == 1)
                                {
                                    // Reintentar carga completa
                                    exito = await ReprocesarCEF(cef, $"{fechaInicioPendiente} - {fechaFinPendiente}", log);
                                }
                                else if (opcion == 2)
                                {
                                    // Reintentar solo conteo
                                    exito = await ReprocesarConteoCEF(cef, $"{fechaInicioPendiente} - {fechaFinPendiente}", log);
                                }
                            }
                            catch (Exception ex)
                            {
                                errorMsg = ex.Message;
                                log.WriteLine($"Error al validar CEF {cef}: {ex.Message}");
                            }

                            if (exito)
                            {
                                log.WriteLine($"CEF {cef} validado exitosamente. Se eliminará de log_carga_cef.");
                                await EliminarRegistroLogCEF(sqlConnection, cef, fechaInicioPendiente, fechaFinPendiente);
                            }
                            else
                            {
                                log.WriteLine($"CEF {cef} no pudo ser validado. Error: {errorMsg}");
                                await ActualizarRegistroLogCEF(sqlConnection, cef, fechaInicioPendiente, fechaFinPendiente);
                            }
                        }
                    }
                }
            }

            // Reintentar los CEFs que no pudieron ser procesados
            await ReintentarCefsFallidos(log);
        }
    }


    // Método para mostrar el menú de opciones
    static int MostrarMenuConTemporizador()
    {
        int opcion = -1;
        int tiempoRestante = 20; // 20 segundos de espera

        Console.WriteLine("Seleccione una opción:");
        Console.WriteLine("1. Realizar Proceso de Carga CEF Completo");
        Console.WriteLine("2. Verificar Conteo de Registros por CEF");
        Console.WriteLine("3. Validar Registros entre FA_POS y Cointech");
        Console.WriteLine();

        // Inicia el temporizador para la interacción
        while (tiempoRestante > 0 && opcion == -1)
        {
            if (Console.KeyAvailable) // Detecta si una tecla fue presionada
            {
                string input = Console.ReadLine();
                if (int.TryParse(input, out opcion) && (opcion == 1 || opcion == 2 || opcion == 3))
                {
                    return opcion; // Opción válida
                }
                else
                {
                    opcion = -1; // Resetea si la opción es inválida
                    Console.WriteLine("Opción no válida. Intente nuevamente.");
                }
            }

            // Mostrar el tiempo restante
            Console.SetCursorPosition(0, Console.CursorTop); // Posicionar el cursor para que sobreescriba la línea anterior
            Console.Write($"El sistema procesará automáticamente en {tiempoRestante} segundos...   ");
            Thread.Sleep(1000); // Espera 1 segundo
            tiempoRestante--;
        }

        // Si no hubo interacción, retorna 0 para la ejecución automática
        return 0;
    }



    static async Task ValidarRegistrosCEF(string fechaInicio, string fechaFin)
    {
        // Abrir el log para registros
        string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
        string logsDirectory = Path.Combine(baseDirectory, "LOGS");

        string fechaActual = DateTime.Now.ToString("yyyyMMdd");

        // Crear la carpeta LOGS si no existe
        if (!Directory.Exists(logsDirectory))
        {
            Directory.CreateDirectory(logsDirectory);
        }

        List<string> cefsPendientes = await ObtenerCefsPendientesFA_POS(fechaInicio, fechaFin);

        string logFilePath = Path.Combine(logsDirectory, $"validacion_cef_{fechaActual}.txt");

        //string logFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\validacion_cef_{DateTime.Now.ToString("yyyyMMdd")}.txt";

        using (StreamWriter log = new StreamWriter(logFilePath, true))
        {
            log.WriteLine("Validación de CEFs faltantes entre FA_POS y Cointech");

            foreach(var cef in cefsPendientes)
            {
                var(ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);

                if (string.IsNullOrEmpty(ipserver))
                {
                    log.WriteLine($"CEF {cef} no encontrado o no activo.");
                    continue;
                }

                // Llamar al proceso de carga para este CEF si está activo
                log.WriteLine($"CEF {cef} encontrado y activo. Procesando...");
                await ProcesarCefs(new List<(string, string, string, string, string)>{ (cef, ipserver, rutadb, userdb, passdb) }, fechaInicio, fechaFin, log, new List<(string, int, double)>());
            }
        }
    }

    static async Task<List<string>> ObtenerCefsPendientesFA_POS(string fechaInicio, string fechaFin)
    {
        List<string> cefsPendientes = new List<string>();
        string sqlServerConnectionString = "Server=192.168.0.174;Database=GSSAP2010;User Id=sa;Password=P@ssw0rd;";

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            await sqlConnection.OpenAsync();
            string query = @"
                declare @tab table(cef nvarchar(10), fec date, tic_pos int, tic_coin int)

                --Insertar en @tab los tickets de FA_POS
                insert into @tab
                SELECT[clocal], [fecha], count([fecha]) AS num_tickete_fapos, 0 AS num_tickete_cointe
                FROM[GSSAP2010].[dbo].[FA_POS]
                WHERE fecha BETWEEN @fechaInicio AND @fechaFin
                GROUP BY[clocal], fecha
                ORDER BY fecha, clocal;

            --Insertar en @tab los tickets de Cointech
                insert into @tab
                SELECT[CEF], CAST([FECHA] AS date) AS fecha, 0 AS num_tickete_fapos, count([FECHA]) AS num_tickete_cointe
                FROM[COINTECH_DB].[dbo].[tickets_db_cointech_cef]
                GROUP BY CEF, fecha
                ORDER BY fecha, CEF;

            --Seleccionar agrupado por CEF y fecha, mostrando los tickets de Cointech = 0, ordenando por fecha y aplicando un rango de fechas
                SELECT cef, fec,
                SUM(tic_coin) AS 'Tickets Cointech',
                SUM(tic_pos) AS 'Tickets Fa_pos',
                SUM(tic_coin) - SUM(tic_pos) AS 'Diferencia'
                FROM @tab
                WHERE fec BETWEEN @fechaInicio AND @fechaFin
                GROUP BY cef, fec
                HAVING SUM(tic_coin) = 0 --Filtrar donde los tickets de Cointech sean 0
                ORDER BY cef, fec; --Ordenar por fecha y luego por CEF";

                using (var command = new SqlCommand(query, sqlConnection))
            {
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        // Aquí asumimos que el campo "cef" es el que contiene la información que necesitas agregar a la lista
                        cefsPendientes.Add(reader["cef"].ToString());
                    }
                }
            }
        }

        return cefsPendientes;
    }



    // Método para obtener lista de CEFs
    static List<string> ObtenerListaCefs()
    {
        Console.Write("Ingrese el/los CEF(s) que desea consultar (separados por comas si son múltiples, o 'TODOS' para consultar todos): ");
        string input = Console.ReadLine();

        if (input.ToUpper().Trim() == "TODOS")
            return new List<string>{ "TODOS" };

        return input.Split(new[]{ ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(cef = > cef.Trim().ToUpper())
            .ToList();
    }

    // Método de reintentos con timeout
    static async Task<int> ProcesarCEFConReintentos(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        int intento = 0;
        int totalRegistrosInsertados = 0;
        int delay = 2000; // Tiempo inicial de espera en milisegundos.
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        while (intento < MAX_RETRIES)
        {
            try
            {
                intento++;
                //log.WriteLine($"[INFO] Intento {intento} de {MAX_RETRIES} para procesar CEF {cef}");

                // Cada intento tiene su propia conexión y transacción
                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    await sqlConnection.OpenAsync();

                    using (var sqlTransaction = sqlConnection.BeginTransaction())
                    {
                        try
                        {
                            // Procesar el CEF
                            totalRegistrosInsertados = await ProcesarCEFConLogAsync(cef, ipserver, rutadb, userdb, passdb, fechaInicio, fechaFin, log);

                            // Si el proceso fue exitoso, registrar el éxito y salir del bucle
                            string mensajeExito = "No presentó error";
                            bool exito = true;
                            await RegistrarErrorCEF(sqlConnection, sqlTransaction, cef, mensajeExito, fechaInicio, fechaFin, exito, log);

                            // Si todo está bien, hacemos commit y salimos
                            sqlTransaction.Commit();
                            return totalRegistrosInsertados;
                        }
                        catch (Exception ex)
                        {
                            // Si hay un error, realizar rollback de la transacción
                            sqlTransaction.Rollback();
                            throw; // Propagar el error para manejarlo fuera
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                log.WriteLine($"[ERROR] Error en el intento {intento} para CEF {cef}: {ex.Message}");

                // Si es el último intento, registrar como fallo
                if (intento >= MAX_RETRIES)
                {
                    log.WriteLine($"[ERROR] Fallo al procesar CEF {cef} después de {MAX_RETRIES} intentos.");

                    // Intentamos registrar el fallo en la tabla log_carga_cef
                    try
                    {
                        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                        {
                            await sqlConnection.OpenAsync();
                            using (var sqlTransaction = sqlConnection.BeginTransaction())
                            {
                                bool exito = false;
                                await RegistrarErrorCEF(sqlConnection, sqlTransaction, cef, ex.Message, fechaInicio, fechaFin, exito, log);
                                sqlTransaction.Commit();
                            }
                        }
                    }
                    catch (Exception rollbackEx)
                    {
                        log.WriteLine($"[ERROR] Error al registrar el fallo del CEF {cef}: {rollbackEx.Message}");
                    }
                    break;
                }
                else
                {
                    // Espera antes de reintentar (backoff exponencial)
                    await Task.Delay(delay);
                    delay *= 2; // Incrementar el tiempo de espera
                }
            }
        }

        return totalRegistrosInsertados;
    }


    // Método para verificar el conteo de registros por CEF mes a mes
    static async Task VerificarConteoRegistrosPorCEF(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefsProcesados, string fechaInicio, string fechaFin, string logVerificationFilePath)
    {
        using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
        {
            // Registro de inicio del proceso de verificación
            logVerification.WriteLine($"Proceso de verificación iniciado: {DateTime.Now.ToString("yyyy - MM - dd HH : mm:ss")}");

            // Conexión a SQL Server para insertar en log_carga_cef_detallado
            string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

            foreach(var cefDatos in listaCefsProcesados)
            {
                DateTime inicioCEF = DateTime.Now;
                logVerification.WriteLine("*****************************");
                logVerification.WriteLine($"CEF {cefDatos.sigla}:");

                int registrosSqlServer = 0;
                int registrosFirebird = 0;
                string status = "EXITO";
                string carga = "OK";

                try
                {
                    // Realizar consulta para el rango de fechas completo (fechaInicio - fechaFin)
                    logVerification.WriteLine($"Rango de consulta: {fechaInicio} a {fechaFin}");

                    registrosSqlServer = await ObtenerConteoSqlServer(cefDatos.sigla, fechaInicio, fechaFin);
                    registrosFirebird = await ObtenerConteoFirebird(cefDatos, fechaInicio, fechaFin);

                    // Verificación inicial
                    logVerification.WriteLine($"Registros consultados en SQL Server: {registrosSqlServer}");
                    logVerification.WriteLine($"Registros consultados en Firebird: {registrosFirebird}");

                    // Si los conteos son distintos, realizar una segunda verificación
                    if (registrosSqlServer != registrosFirebird)
                    {
                        logVerification.WriteLine("*** Conteo del CEF distinto, se volverá a proceder el conteo. ***");

                        // Segunda verificación
                        registrosSqlServer = await ObtenerConteoSqlServer(cefDatos.sigla, fechaInicio, fechaFin);
                        registrosFirebird = await ObtenerConteoFirebird(cefDatos, fechaInicio, fechaFin);

                        logVerification.WriteLine($"[Reintento] Registros en SQL Server: {registrosSqlServer}");
                        logVerification.WriteLine($"[Reintento] Registros en Firebird: {registrosFirebird}");

                        // Si siguen siendo diferentes después del segundo intento
                        if (registrosSqlServer != registrosFirebird)
                        {
                            logVerification.WriteLine("*** Conteo del CEF sigue siendo distinto. Reportando como inconsistente. ***");
                            logVerification.WriteLine($"Diferencia SQL Server: {registrosSqlServer} - Firebird: {registrosFirebird}");
                            status = "WARNING";
                            carga = "0";  // Conteos distintos, advertencia
                        }
                        else
                        {
                            logVerification.WriteLine("*** El conteo coincidió tras el reintento. ***");
                        }
                    }
                    else
                    {
                        logVerification.WriteLine("*** El conteo fue exitoso en el primer intento. ***");
                    }
                }
                catch (SqlException sqlEx)
                {
                    logVerification.WriteLine($"*** Error de conexión SQL Server para CEF {cefDatos.sigla}: {sqlEx.Message} ***");
                    status = "ERROR";
                    carga = "0";  // Error en la conexión SQL Server
                }
                catch (OdbcException odbcEx)
                {
                    logVerification.WriteLine($"*** Error de conexión Firebird para CEF {cefDatos.sigla}: {odbcEx.Message} ***");
                    status = "ERROR";
                    carga = "0";  // Error en la conexión Firebird
                }
                catch (Exception ex)
                {
                    logVerification.WriteLine($"*** Error general para CEF {cefDatos.sigla}: {ex.Message} ***");
                    status = "ERROR";
                    carga = "0";  // Error general
                }

                DateTime finCEF = DateTime.Now;
                TimeSpan duracionCEF = finCEF - inicioCEF;
                string tiempoFormateadoCEF = $"{duracionCEF.Hours} horas, {duracionCEF.Minutes} minutos, {duracionCEF.Seconds} segundos";

                logVerification.WriteLine($"Inicio de la consulta: {inicioCEF.ToString("yyyy - MM - dd HH : mm:ss")}");
                logVerification.WriteLine($"Fin de la consulta: {finCEF.ToString("yyyy - MM - dd HH : mm:ss")}");
                logVerification.WriteLine($"Tiempo total de consulta para el CEF {cefDatos.sigla}: {tiempoFormateadoCEF}");
                logVerification.WriteLine("*****************************");

                // Insertar en la tabla log_carga_cef_detallado
                /*
                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    await sqlConnection.OpenAsync();

                    string insertQuery = @"
                    INSERT INTO COINTECH_DB.dbo.log_carga_cef_detallado
                    (CEF, rangoConsulta, registrosSqlServer, registrosFirebird, status, carga, fechaRegistro)
                    VALUES
                    (@CEF, @rangoConsulta, @registrosSqlServer, @registrosFirebird, @status, @carga, GETDATE())";

                    using (var command = new SqlCommand(insertQuery, sqlConnection))
                    {
                        command.Parameters.AddWithValue("@CEF", cefDatos.sigla);
                        command.Parameters.AddWithValue("@rangoConsulta", $"{fechaInicio} - {fechaFin}");
                        command.Parameters.AddWithValue("@registrosSqlServer", registrosSqlServer);
                        command.Parameters.AddWithValue("@registrosFirebird", registrosFirebird);
                        command.Parameters.AddWithValue("@status", status);
                        command.Parameters.AddWithValue("@carga", carga);

                        await command.ExecuteNonQueryAsync();
                    }
                }
                */
            }

            // Registro del fin del proceso de verificación
            logVerification.WriteLine($"Proceso de verificación finalizado: {DateTime.Now.ToString("yyyy - MM - dd HH : mm:ss")}");
        }
    }


    static async Task<int> ObtenerConteoSqlServer(string cef, string fechaInicio, string fechaFin)
    {
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int count = 0;

        // Definir la ruta del log directamente aquí
        string logVerificationFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log_verificacion.txt");

        try
        {
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();

                string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
                    WHERE CEF = @CEF AND CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin; ";

                    using (var command = new SqlCommand(query, sqlConnection))
                {
                    command.Parameters.AddWithValue("@CEF", cef);
                    command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                    command.Parameters.AddWithValue("@fechaFin", fechaFin);

                    count = (int)await command.ExecuteScalarAsync();
                }
            }
        }
        catch (SqlException ex)
        {
            // Registrar el error directamente en el log aquí
            using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
            {
                logVerification.WriteLine($"*** Error de SQL Server para CEF {cef}: {ex.Message} ***");
            }
        }
        catch (Exception ex)
        {
            // Registrar cualquier otro error directamente en el log
            using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
            {
                logVerification.WriteLine($"*** Error general para CEF {cef}: {ex.Message} ***");
            }
        }

        return count;
    }


    static async Task<int> ObtenerConteoFirebird((string sigla, string ipserver, string rutadb, string userdb, string passdb) cefDatos, string fechaInicio, string fechaFin)
    {
        int count = 0;

        // Definir la ruta del log directamente aquí
        string logVerificationFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log_verificacion.txt");

        try
        {
            string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={cefDatos.ipserver}/3050:{cefDatos.rutadb};UID={cefDatos.userdb};PWD={cefDatos.passdb};";
            string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

            // Obtener el valor de 'region' para este CEF desde SQL Server
            string region = null;
            string regionQuery = "SELECT region FROM ADM_CEFS WHERE sigla = @cef and activo = 1";

            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();
                using (var command = new SqlCommand(regionQuery, sqlConnection))
                {
                    command.Parameters.AddWithValue("@cef", cefDatos.sigla);
                    var result = await command.ExecuteScalarAsync();
                    region = result != DBNull.Value ? result ? .ToString() : null;
                }
            }

            // Definir la consulta de Firebird basada en el valor de 'region'
            string query;

            if (region == "UP")
            {
                // Consulta específica para CEFs con region "UP"
                query = @"SELECT COUNT(*) FROM caj_turnos a
                    INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                    INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                    WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                    AND b.importe < > 0
                    AND c.numcomprobante > 0
                    AND a.numerocaja - 200 >= 20; ";
            }
            else
            {
                // Consulta para CEFs con region distinta de "UP"
                query = @"SELECT COUNT(*) FROM caj_turnos a
                    INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                    INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                    WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                    AND b.importe < > 0
                    AND c.numcomprobante > 0
                    AND a.numerocaja - 200 < 20; ";
            }

            // Ejecutar la consulta en Firebird
            using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
            {
                await firebirdConnection.OpenAsync();
                using (var command = new OdbcCommand(query, firebirdConnection))
                {
                    command.CommandTimeout = 600;
                    command.Parameters.AddWithValue("?", fechaInicio);
                    command.Parameters.AddWithValue("?", fechaFin);

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && int.TryParse(result.ToString(), out int countResult))
                    {
                        count = countResult;
                    }
                }
            }
        }
        catch (OdbcException ex)
        {
            // Registrar el error directamente en el log aquí
            using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
            {
                logVerification.WriteLine($"*** Error de Firebird para CEF {cefDatos.sigla}: {ex.Message} ***");
            }
        }
        catch (Exception ex)
        {
            // Registrar cualquier otro error directamente en el log
            using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
            {
                logVerification.WriteLine($"*** Error general para CEF {cefDatos.sigla}: {ex.Message} ***");
            }
        }

        return count;
    }

    //Método para reintentar los CEFs que fallaron en el proceso anterior
    static async Task ReintentarCefsFallidos(StreamWriter log)
    {
        using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
        {
            await sqlConnection.OpenAsync();
            string query = "SELECT CEF, fechaInicio, fechaFin FROM log_carga_cef WHERE estado = 'Fallido'";
            using (var command = new SqlCommand(query, sqlConnection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    List<(string cef, string fechaInicio, string fechaFin)> cefsFallidos = new List<(string cef, string fechaInicio, string fechaFin)>();

                    while (reader.Read())
                    {
                        string cef = reader["CEF"].ToString();
                        string fechaInicio = reader["fechaInicio"].ToString();
                        string fechaFin = reader["fechaFin"].ToString();

                        cefsFallidos.Add((cef, fechaInicio, fechaFin));
                    }

                    foreach(var(cef, fechaInicio, fechaFin) in cefsFallidos)
                    {
                        log.WriteLine($"Reintentando CEF fallido: {cef} en el rango {fechaInicio} - {fechaFin}");

                        // Intentar procesar el CEF nuevamente
                        bool exito = await ReprocesarCEF(cef, $"{fechaInicio} - {fechaFin}", log);

                        if (exito)
                        {
                            log.WriteLine($"CEF {cef} procesado correctamente. Eliminando de log_carga_cef.");
                            await EliminarRegistroLogCEF(sqlConnection, cef, fechaInicio, fechaFin);
                        }
                        else
                        {
                            log.WriteLine($"CEF {cef} falló nuevamente en el reintento. No se volverá a intentar.");
                            await ActualizarRegistroLogCEF(sqlConnection, cef, fechaInicio, fechaFin);
                        }
                    }
                }
            }
        }
    }



    static async Task<bool> ReprocesarCEF(string cef, string fechaRango, StreamWriter log)
    {
        string fechaInicio = string.Empty;
        string fechaFin = string.Empty;
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        try
        {
            var(ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);
            if (string.IsNullOrEmpty(ipserver))
            {
                log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}' durante la validación.");
                return false;
            }

            // Separar las fechas inicio y fin del rango
            string[] fechas = fechaRango.Split('-');
            fechaInicio = fechas[0].Trim();
            fechaFin = fechas[1].Trim();

            List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCef = new List<(string, string, string, string, string)>
            {
                (cef, ipserver, rutadb, userdb, passdb)
            };

            List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();

            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();

                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    try
                    {
                        // Procesar el CEF nuevamente
                        await ProcesarCefs(listaCef, fechaInicio, fechaFin, log, detallesCefs);

                        // Si el proceso es exitoso, registrar éxito
                        bool exito = true;
                        string mensajeExito = "No presentó error";
                        await RegistrarErrorCEF(sqlConnection, sqlTransaction, cef, mensajeExito, fechaInicio, fechaFin, exito, log); // Cambiado para usar conexión/transacción

                        // Commit de la transacción si todo salió bien
                        sqlTransaction.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        log.WriteLine($"Error procesando CEF {cef} durante la validación: {ex.Message}");

                        // Registrar el error en la tabla log_carga_cef
                        bool exito = false;
                        await RegistrarErrorCEF(sqlConnection, sqlTransaction, cef, ex.Message, fechaInicio, fechaFin, exito, log); // Cambiado para usar conexión/transacción

                        // Rollback de la transacción en caso de error
                        sqlTransaction.Rollback();
                        return false;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error general al procesar el CEF {cef}: {ex.Message}");
            return false;
        }
    }




    // Método para reprocesar solo el conteo de un CEF
    static async Task<bool> ReprocesarConteoCEF(string cef, string fechaRango, StreamWriter log)
    {

        // Abrir el log para registros
        string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
        string logsDirectory = Path.Combine(baseDirectory, "LOGS");

        string fechaActual = DateTime.Now.ToString("yyyyMMdd");

        // Crear la carpeta LOGS si no existe
        if (!Directory.Exists(logsDirectory))
        {
            Directory.CreateDirectory(logsDirectory);
        }

        try
        {
            var(ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);
            if (string.IsNullOrEmpty(ipserver))
            {
                log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}' durante la validación.");
                return false;
            }

            // Separar las fechas inicio y fin del rango
            string[] fechas = fechaRango.Split('-');
            string fechaInicio = fechas[0].Trim();
            string fechaFin = fechas[1].Trim();

            List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCef = new List<(string, string, string, string, string)>
            {
                (cef, ipserver, rutadb, userdb, passdb)
            };

            //string logVerificationFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\validacion_error_carga_cef {DateTime.Now.ToString("yyyyMMdd")}.txt";

            string logVerificationFilePath = Path.Combine(logsDirectory, $"validacion_error_carga_cef_{fechaActual}.txt");
            await VerificarConteoRegistrosPorCEF(listaCef, fechaInicio, fechaFin, logVerificationFilePath);

            return true;
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error procesando conteo de CEF {cef} durante la validación: {ex.Message}");
            return false;
        }
    }

    static async Task EliminarRegistroLogCEF(SqlConnection sqlConnection, string cef, string fechaInicio, string fechaFin)
    {
        string deleteQuery = @"
            DELETE FROM log_carga_cef
            WHERE CEF = @CEF AND fechaInicio = @fechaInicio AND fechaFin = @fechaFin";

            try
        {
            using (var command = new SqlCommand(deleteQuery, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                int rowsAffected = await command.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    Console.WriteLine($"Registro eliminado correctamente para el CEF: {cef} en el rango {fechaInicio} - {fechaFin}");
                }
                else
                {
                    Console.WriteLine($"No se encontró un registro para eliminar para el CEF: {cef} en el rango {fechaInicio} - {fechaFin}");
                }
            }
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Error al eliminar el registro del CEF {cef}: {ex.Message}");
        }
    }


    static async Task ActualizarRegistroLogCEF(SqlConnection sqlConnection, string cef, string fechaInicio, string fechaFin)
    {
        string updateQuery = @"
            UPDATE log_carga_cef
            SET estado = 'Completo', reintentos = reintentos + 1
            WHERE CEF = @CEF AND fechaInicio = @fechaInicio AND fechaFin = @fechaFin AND estado = 'Fallido'";

            try
        {
            using (var command = new SqlCommand(updateQuery, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                int rowsAffected = await command.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    Console.WriteLine($"Registro actualizado correctamente para el CEF: {cef} en el rango {fechaInicio} - {fechaFin}");
                }
                else
                {
                    Console.WriteLine($"No se encontró un registro para actualizar para el CEF: {cef} en el rango {fechaInicio} - {fechaFin}");
                }
            }
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Error actualizando el registro del CEF {cef}: {ex.Message}");
        }
    }



    static async Task<TimeSpan> ProcesarCefs(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, string fechaInicio, string fechaFin, StreamWriter log, List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs)
    {
        TimeSpan tiempoTotal = TimeSpan.Zero; // Acumula el tiempo total de procesamiento

        // Configurar la conexión a la base de datos para insertar en log_carga_cef_detallado
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        foreach(var cefDatos in listaCefs)
        {
            // Mostrar en la consola el CEF que se está procesando
            Console.WriteLine($"Procesando CEF: {cefDatos.sigla}...");

            // Iniciar el procesamiento de un solo CEF y registrar el tiempo
            DateTime inicioCEF = DateTime.Now;
            log.WriteLine("*****************************");
            log.WriteLine($"CEF: {cefDatos.sigla}");
            log.WriteLine($"Inicio del procesamiento: {inicioCEF.ToString("yyyy - MM - dd HH : mm:ss")}");
            log.WriteLine($"Fechas a procesar: {fechaInicio} - {fechaFin}");

            int totalRegistrosInsertados = 0;
            int registrosSqlServer = 0;
            int registrosFirebird = 0;
            bool fallo = false;
            string mensajeFallo = string.Empty;
            string status = "EXITO";
            string carga = "OK";

            try
            {
                // Procesa el CEF y obtiene los registros
                totalRegistrosInsertados = await ProcesarCEFConLogAsync(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin, log);

                // Simulación de obtención de registros de SQL Server y Firebird (debes reemplazar con tu lógica real)
                registrosSqlServer = await ObtenerConteoSqlServer(cefDatos.sigla, fechaInicio, fechaFin);
                registrosFirebird = await ObtenerConteoFirebird(cefDatos, fechaInicio, fechaFin);

                // Comparar registros y establecer estado de carga
                if (registrosSqlServer != registrosFirebird)
                {
                    status = "WARNING"; // Si los registros son distintos, advertencia
                    carga = "0"; // La carga no fue exitosa
                }

            }
            catch (Exception ex)
            {
                fallo = true;
                mensajeFallo = $"Fallo: {ex.Message}";
                status = "ERROR";
                carga = "0"; // Si hay un fallo, la carga es 0
            }

            DateTime finCEF = DateTime.Now;
            TimeSpan duracionProceso = finCEF - inicioCEF;

            // Sumar el tiempo de procesamiento de este CEF al acumulador
            tiempoTotal += duracionProceso;

            string tiempoFormateado = $"{duracionProceso.Hours} horas, {duracionProceso.Minutes} minutos, {duracionProceso.Seconds} segundos";

            if (fallo)
            {
                log.WriteLine($"Status: {mensajeFallo}");
                log.WriteLine($"Tiempo total del procesamiento fallido para CEF {cefDatos.sigla}: {tiempoFormateado}");
            }
            else
            {
                log.WriteLine($"Status: No presentó problemas de inserción, se registraron: {totalRegistrosInsertados} registros");
                log.WriteLine($"Tiempo total del procesamiento para CEF {cefDatos.sigla}: {tiempoFormateado}");
            }

            log.WriteLine($"Fin del procesamiento para CEF {cefDatos.sigla}: {finCEF.ToString("yyyy - MM - dd HH : mm:ss")}");
            log.WriteLine("*****************************");

            // Mostrar en consola que finalizó el procesamiento del CEF
            Console.WriteLine($"CEF {cefDatos.sigla} procesado.");
            Console.WriteLine($"Tiempo total del procesamiento del CEF {cefDatos.sigla}: {tiempoFormateado}");

            // Guardar detalles para el resumen final
            detallesCefs.Add((cefDatos.sigla, totalRegistrosInsertados, duracionProceso.TotalSeconds));

            // Insertar en la tabla log_carga_cef_detallado
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();

                string insertQuery = @"
                    INSERT INTO COINTECH_DB.dbo.log_carga_cef_detallado
                    (CEF, rangoConsulta, registrosSqlServer, registrosFirebird, status, carga, fechaRegistro)
                    VALUES
                    (@CEF, @rangoConsulta, @registrosSqlServer, @registrosFirebird, @status, @carga, GETDATE())";

                    using (var command = new SqlCommand(insertQuery, sqlConnection))
                {
                    command.Parameters.AddWithValue("@CEF", cefDatos.sigla);
                    command.Parameters.AddWithValue("@rangoConsulta", $"{fechaInicio} - {fechaFin}");
                    command.Parameters.AddWithValue("@registrosSqlServer", registrosSqlServer);
                    command.Parameters.AddWithValue("@registrosFirebird", registrosFirebird);
                    command.Parameters.AddWithValue("@status", status);
                    command.Parameters.AddWithValue("@carga", carga);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        // Retornar el tiempo total acumulado de los CEFs procesados
        return tiempoTotal;
    }



    static async Task<int> ProcesarCEFConLogAsync(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={ipserver}/3050:{rutadb};UID={userdb};PWD={passdb};";
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int totalRegistrosInsertados = 0;

        int registrosSqlServer = 0;
        int registrosFirebird = 0;
        string status = "EXITO";
        string carga = "OK";

        try
        {
            using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await firebirdConnection.OpenAsync();
                //log.WriteLine("[INFO] Conexión Firebird abierta exitosamente");
                await sqlConnection.OpenAsync();
                //log.WriteLine("[INFO] Conexión SQL Server abierta exitosamente");

                using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction())
                {
                    try
                    {
                        DateTime fechaInicioDT = DateTime.Parse(fechaInicio);
                        DateTime fechaFinDT = DateTime.Parse(fechaFin);

                        while (fechaInicioDT <= fechaFinDT)
                        {
                            string fechaActual = fechaInicioDT.ToString("yyyy-MM-dd");
                            //log.WriteLine($"[INFO] Procesando registros para la fecha {fechaActual}...");

                            // Realizar proceso de carga por día
                            int registrosDia = await RealizarProcesoDeCargaPorDia(firebirdConnection, sqlConnection, cef, fechaActual, fechaActual, sqlTransaction, log);
                            totalRegistrosInsertados += registrosDia;

                            //log.WriteLine($"[INFO] Registros insertados para {fechaActual}: {registrosDia}");

                            fechaInicioDT = fechaInicioDT.AddDays(1);
                        }

                        // Commit de la transacción si todo salió bien
                        sqlTransaction.Commit();
                        //log.WriteLine("[INFO] Commit realizado correctamente en SQL Server");
                    }
                    catch (Exception ex)
                    {
                        log.WriteLine($"[ERROR] Ocurrió un error durante el proceso. Realizando rollback... Detalles: {ex.Message}");
                        if (sqlTransaction.Connection != null)
                        {
                            sqlTransaction.Rollback();
                        }
                        throw;
                    }
                }

                // Obtener registros de SQL Server y Firebird
                registrosSqlServer = await ObtenerConteoSqlServer(cef, fechaInicio, fechaFin);
                registrosFirebird = await ObtenerConteoFirebird((cef, ipserver, rutadb, userdb, passdb), fechaInicio, fechaFin);

                // Evaluar el estado de la carga
                if (registrosSqlServer != registrosFirebird)
                {
                    status = "WARNING";
                    carga = "0";  // Si hay diferencias entre los registros
                }
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"[ERROR] Error al procesar el CEF {cef}: {ex.Message}");
            status = "ERROR";
            carga = "0";  // Si hay un error, la carga es 0
            throw;
        }

        // Insertar en la tabla log_carga_cef_detallado
        /*
        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            await sqlConnection.OpenAsync();

            string insertQuery = @"
            INSERT INTO COINTECH_DB.dbo.log_carga_cef_detallado
            (CEF, rangoConsulta, registrosSqlServer, registrosFirebird, status, carga, fechaRegistro)
            VALUES
            (@CEF, @rangoConsulta, @registrosSqlServer, @registrosFirebird, @status, @carga, GETDATE())";

            using (var command = new SqlCommand(insertQuery, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@rangoConsulta", $"{fechaInicio} - {fechaFin}");
                command.Parameters.AddWithValue("@registrosSqlServer", registrosSqlServer);
                command.Parameters.AddWithValue("@registrosFirebird", registrosFirebird);
                command.Parameters.AddWithValue("@status", status);
                command.Parameters.AddWithValue("@carga", carga);

                await command.ExecuteNonQueryAsync();
            }
        }
        */


        return totalRegistrosInsertados;
    }



    static async Task<int> ObtenerConteoRegistrosSqlServer(SqlConnection sqlConnection, string cef, string fechaActual, SqlTransaction transaction, StreamWriter log)
    {
        string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
            WHERE CEF = @CEF AND CAST(FECHA AS DATE) = @Fecha";

            // Verificar si el objeto `log` no es nulo antes de escribir en él
            if (log != null)
            {
                //log.WriteLine($"[INFO] Iniciando conteo de registros en SQL Server para CEF: {cef}, Fecha: {fechaActual}");
            }

        // Asegurarse de que la conexión a SQL Server esté abierta
        if (sqlConnection.State != ConnectionState.Open)
        {
            await sqlConnection.OpenAsync();
            if (log != null)
            {
                //log.WriteLine("[INFO] Conexión a SQL Server abierta exitosamente.");
            }
        }

        using (var command = new SqlCommand(query, sqlConnection, transaction))
        {
            command.Parameters.AddWithValue("@CEF", cef);
            command.Parameters.AddWithValue("@Fecha", fechaActual);

            var result = await command.ExecuteScalarAsync();
            int conteo = Convert.ToInt32(result);

            // Registrar el resultado en el log
            if (log != null)
            {
                //log.WriteLine($"[INFO] Conteo de registros en SQL Server para CEF {cef}, Fecha {fechaActual}: {conteo}");
            }

            return conteo;
        }
    }



    static async Task<int> ObtenerConteoRegistrosFirebird(OdbcConnection firebirdConnection, string cef, string fechaInicio, string fechaFin, StreamWriter log)
    {
        string query;

        // Cadena de conexión a SQL Server
        string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

        // Obtener el valor de 'region' para el CEF proporcionado
        string region = null;
        string regionQuery = "SELECT region FROM ADM_CEFS WHERE sigla = @cef and activo = 1";

        try
        {
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();
                using (var command = new SqlCommand(regionQuery, sqlConnection))
                {
                    command.Parameters.AddWithValue("@cef", cef);
                    var result = await command.ExecuteScalarAsync();
                    region = result != DBNull.Value ? result ? .ToString() : null;
                }
            }
        }
        catch (Exception ex)
        {
            log ? .WriteLine($"[ERROR] Error al obtener la región para el CEF {cef}: {ex.Message}");
            throw;
        }

        // Definir la consulta de Firebird basada en el valor de 'region'
        if (region == "UP")
        {
            query = @"SELECT COUNT(*) FROM caj_turnos a
                INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                AND b.importe < > 0
                AND c.numcomprobante > 0
                AND a.numerocaja - 200 >= 20";
        }
        else
        {
            query = @"SELECT COUNT(*) FROM caj_turnos a
                INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                AND b.importe < > 0
                AND c.numcomprobante > 0
                AND a.numerocaja - 200 < 20";
        }

        // Verificar si log no es nulo y registrar el inicio
        if (log != null)
        {
            //log.WriteLine($"[INFO] Iniciando conteo de registros en Firebird para CEF {cef}, rango: {fechaInicio} a {fechaFin}");
        }

        // Asegurarse de que la conexión a Firebird esté abierta
        if (firebirdConnection.State != ConnectionState.Open)
        {
            await firebirdConnection.OpenAsync();
            if (log != null)
            {
                //log.WriteLine("[INFO] Conexión a Firebird abierta exitosamente.");
            }
        }

        // Ejecutar la consulta en Firebird
        using (var command = new OdbcCommand(query, firebirdConnection))
        {
            command.Parameters.AddWithValue("?", fechaInicio);
            command.Parameters.AddWithValue("?", fechaFin);

            try
            {
                var result = await command.ExecuteScalarAsync();
                int conteo = Convert.ToInt32(result);

                // Registrar el resultado en el log
                if (log != null)
                {
                    //log.WriteLine($"[INFO] Conteo de registros en Firebird para CEF {cef}, rango {fechaInicio} - {fechaFin}: {conteo}");
                }

                return conteo;
            }
            catch (Exception ex)
            {
                if (log != null)
                {
                    log.WriteLine($"[ERROR] Error en Firebird para CEF {cef}: {ex.Message}");
                }
                throw; // Re-lanzar la excepción
            }
        }
    }

    static async Task<int> RealizarProcesoDeCargaPorDia(OdbcConnection firebirdConnection, SqlConnection sqlConnection, string cef, string fechaInicio, string fechaFin, SqlTransaction transaction, StreamWriter log)
    {
        int batchSize = 10000;  // Número de registros por lote para la inserción masiva
        int totalRegistrosInsertados = 0;
        string query;

        // Consulta para obtener el valor de 'region' para el CEF proporcionado
        string region = null;
        string regionQuery = "SELECT region FROM ADM_CEFS WHERE sigla = @cef and activo = 1";

        // Cadena de conexión a SQL Server para acceder a la tabla ADM_CEFS
        string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

        try
        {
            // Abre la conexión a SQL Server y ejecuta la consulta para obtener el valor de 'region'
            using (var sqlConnectionForRegion = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnectionForRegion.OpenAsync();
                using (var command = new SqlCommand(regionQuery, sqlConnectionForRegion))
                {
                    command.Parameters.AddWithValue("@cef", cef);
                    var result = await command.ExecuteScalarAsync();
                    region = result != DBNull.Value ? result ? .ToString() : null;
                }
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"[ERROR] Error al obtener la región para el CEF {cef}: {ex.Message}");
            throw;
        }

        // Verificar el valor de 'region' y definir la consulta correspondiente
        if (region == "UP")
        {
            // Consulta específica para CEFs con region "UP"
            query = @"SELECT 
                CAST(b.idtranpos AS VARCHAR(500)) AS id_transaccion,
                CAST(CAST(c.fechayhora AS TIME) AS VARCHAR(500)) AS hora,
                CAST(a.numerocaja - 200 AS VARCHAR(500)) AS numero_terminal,
                CAST(b.tipo AS VARCHAR(500)) AS tipo,
                CAST(c.numcomprobante AS VARCHAR(500)) AS numero_comprobante,
                CAST(b.importe AS VARCHAR(500)) AS importe,
                CAST(a.fechaadministrativa AS DATE) AS fecha
                FROM
                caj_turnos a
                INNER JOIN
                caj_transacciones b ON a.idturnocaja = b.idturnocaja
                INNER JOIN
                pos_transacciones c ON b.idtranpos = c.idtranpos
                WHERE
                CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                AND b.importe < > 0
                AND c.numcomprobante > 0
                AND a.numerocaja - 200 >= 20";
        }
        else
        {
            // Consulta para CEFs con region distinta de "UP" (incluye NULL o valores en blanco)
            query = @"SELECT DISTINCT
                CAST(b.idtranpos AS VARCHAR(500)) AS id_transaccion,
                CAST(CAST(c.fechayhora AS TIME) AS VARCHAR(500)) AS hora,
                CAST(a.numerocaja - 200 AS VARCHAR(500)) AS numero_terminal,
                CAST(b.tipo AS VARCHAR(500)) AS tipo,
                CAST(c.numcomprobante AS VARCHAR(500)) AS numero_comprobante,
                CAST(b.importe AS VARCHAR(500)) AS importe,
                CAST(a.fechaadministrativa AS DATE) AS fecha
                FROM
                caj_turnos a
                INNER JOIN
                caj_transacciones b ON a.idturnocaja = b.idturnocaja
                INNER JOIN
                pos_transacciones c ON b.idtranpos = c.idtranpos
                WHERE
                CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                AND b.importe < > 0
                AND c.numcomprobante > 0
                AND a.numerocaja - 200 < 20";
        }

        try
        {
            // Asegurarse de que la conexión esté abierta para Firebird
            if (firebirdConnection.State != ConnectionState.Open)
            {
                await firebirdConnection.OpenAsync();
                //log.WriteLine("[INFO] Conexión a Firebird abierta exitosamente.");
            }

            using (var command = new OdbcCommand(query, firebirdConnection))
            {
                command.CommandTimeout = 600;  // Establecer tiempo de espera
                command.Parameters.AddWithValue("?", fechaInicio);
                command.Parameters.AddWithValue("?", fechaFin);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    DataTable dataTable = new DataTable();
                    dataTable.Columns.Add("ID_TRANSACCION", typeof(long));
                    dataTable.Columns.Add("HORA", typeof(string));
                    dataTable.Columns.Add("NUMERO_TERMINAL", typeof(int));
                    dataTable.Columns.Add("TIPO", typeof(string));
                    dataTable.Columns.Add("NUMERO_COMPROBANTE", typeof(string));
                    dataTable.Columns.Add("IMPORTE", typeof(decimal));
                    dataTable.Columns.Add("FECHA", typeof(string));
                    dataTable.Columns.Add("FORMA_PAGO", typeof(string));
                    dataTable.Columns.Add("CEF", typeof(string));

                    // Leer los datos de Firebird
                    while (await reader.ReadAsync())
                    {
                        dataTable.Rows.Add(
                            reader["id_transaccion"],
                            reader["hora"].ToString(),
                            reader["numero_terminal"],
                            reader["tipo"],
                            reader["numero_comprobante"],
                            reader["importe"],
                            fechaInicio,  // Usamos fechaInicio, ya que es el día que estamos procesando
                            "01",  // Supongo que "01" es el código para la forma de pago
                            cef  // El CEF actual
                        );
                    }

                    int totalRegistros = dataTable.Rows.Count;
                    //log.WriteLine($"[INFO] Total de registros extraídos desde Firebird para el CEF {cef}: {totalRegistros}");

                    if (totalRegistros > 0)
                    {
                        // Insertar en SQL Server en lotes
                        for (int i = 0; i < totalRegistros; i += batchSize)
                        {
                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction))
                            {
                                bulkCopy.DestinationTableName = "COINTECH_DB.dbo.tickets_db_cointech_cef";
                                bulkCopy.BulkCopyTimeout = 600;  // Establecer tiempo de espera para la inserción
                                bulkCopy.BatchSize = batchSize;

                                // Mapear las columnas
                                bulkCopy.ColumnMappings.Add("ID_TRANSACCION", "ID_TRANSACCION");
                                bulkCopy.ColumnMappings.Add("HORA", "HORA");
                                bulkCopy.ColumnMappings.Add("NUMERO_TERMINAL", "NUMERO_TERMINAL");
                                bulkCopy.ColumnMappings.Add("TIPO", "TIPO");
                                bulkCopy.ColumnMappings.Add("NUMERO_COMPROBANTE", "NUMERO_COMPROBANTE");
                                bulkCopy.ColumnMappings.Add("IMPORTE", "IMPORTE");
                                bulkCopy.ColumnMappings.Add("FECHA", "FECHA");
                                bulkCopy.ColumnMappings.Add("FORMA_PAGO", "FORMA_PAGO");
                                bulkCopy.ColumnMappings.Add("CEF", "CEF");

                                // Obtener el lote actual de registros
                                DataTable batchTable = dataTable.AsEnumerable().Skip(i).Take(batchSize).CopyToDataTable();
                                //log.WriteLine($"[INFO] Intentando insertar {batchTable.Rows.Count} registros en SQL Server para CEF {cef}...");

                                await bulkCopy.WriteToServerAsync(batchTable);
                                totalRegistrosInsertados += batchTable.Rows.Count;

                                //log.WriteLine($"[INFO] Registros insertados hasta ahora para CEF {cef}: {totalRegistrosInsertados}");
                            }
                        }
                    }
                    else
                    {
                        log.WriteLine($"[WARNING] No se encontraron registros en Firebird para el CEF {cef} en el rango {fechaInicio} - {fechaFin}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            //log.WriteLine($"[ERROR] Error procesando CEF {cef} para el día {fechaInicio}: {ex.Message}");
            throw;  // Propagar el error al método que maneja la transacción
        }
        finally
        {
            // Cerrar la conexión Firebird al finalizar el proceso
            if (firebirdConnection.State == ConnectionState.Open)
            {
                firebirdConnection.Close();
                //log.WriteLine("[INFO] Conexión a Firebird cerrada.");
            }
        }

        return totalRegistrosInsertados;
    }




    // Modificado para aceptar la conexión y la transacción existentes
    static async Task RegistrarErrorCEF(SqlConnection sqlConnection, SqlTransaction transaction, string cef, string mensaje, string fechaInicio, string fechaFin, bool exito, StreamWriter log)
    {
        string estado = exito ? "Completo" : "Fallido";
        string status = exito ? "EXITO" : "ERROR";
        string carga = exito ? "OK" : "0";  // Si fue exitoso, carga OK; si falló, carga 0
        string fechaRango = $"{fechaInicio} - {fechaFin}";
        int intentos = 0;
        bool registrado = false;

        // Validar longitud de los campos para evitar errores en la base de datos
        if (cef.Length > 10)
        {
            cef = cef.Substring(0, 10);
            log.WriteLine($"CEF truncado a 10 caracteres: {cef}");
        }

        if (mensaje.Length > 500)
        {
            mensaje = mensaje.Substring(0, 500);
            log.WriteLine($"Mensaje truncado a 500 caracteres.");
        }

        while (intentos < MAX_RETRIES && !registrado)
        {
            try
            {
                // Registrar en la tabla log_carga_cef original
                string insertQuery = @"
                    INSERT INTO log_carga_cef(CEF, tipoError, fechaInicio, fechaFin, estado, reintentos, fechaRango, fechaRegistro)
                    VALUES(@CEF, @tipoError, @fechaInicio, @fechaFin, @estado, @reintentos, @fechaRango, GETDATE())";

                    using (var command = new SqlCommand(insertQuery, sqlConnection, transaction))
                {
                    command.Parameters.AddWithValue("@CEF", cef);
                    command.Parameters.AddWithValue("@tipoError", mensaje);
                    command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                    command.Parameters.AddWithValue("@fechaFin", fechaFin);
                    command.Parameters.AddWithValue("@estado", estado);
                    command.Parameters.AddWithValue("@reintentos", intentos);
                    command.Parameters.AddWithValue("@fechaRango", fechaRango);

                    int rowsAffected = await command.ExecuteNonQueryAsync();

                    if (rowsAffected > 0)
                    {
                        registrado = true;
                        //log.WriteLine($"[INFO] Registro {estado} realizado correctamente para CEF {cef}, Rango: {fechaRango}");
                    }
                    else
                    {
                        log.WriteLine($"[WARNING] La inserción para CEF {cef} no afectó ninguna fila en la tabla log_carga_cef.");
                    }
                }

                // Registrar en la tabla log_carga_cef_detallado
                string insertQueryDetallado = @"
                    INSERT INTO COINTECH_DB.dbo.log_carga_cef_detallado
                    (CEF, rangoConsulta, registrosSqlServer, registrosFirebird, status, carga, fechaRegistro)
                    VALUES
                    (@CEF, @rangoConsulta, @registrosSqlServer, @registrosFirebird, @status, @carga, GETDATE())";

                    using (var commandDetallado = new SqlCommand(insertQueryDetallado, sqlConnection, transaction))
                {
                    commandDetallado.Parameters.AddWithValue("@CEF", cef);
                    commandDetallado.Parameters.AddWithValue("@rangoConsulta", fechaRango);
                    commandDetallado.Parameters.AddWithValue("@registrosSqlServer", 0); // No se registran conteos en este momento
                    commandDetallado.Parameters.AddWithValue("@registrosFirebird", 0);  // No se registran conteos en este momento
                    commandDetallado.Parameters.AddWithValue("@status", status);
                    commandDetallado.Parameters.AddWithValue("@carga", carga);

                    await commandDetallado.ExecuteNonQueryAsync();
                }
            }
            catch (SqlException sqlEx)
            {
                log.WriteLine($"[ERROR] SQL Exception al registrar {estado} para CEF {cef}, intento {intentos + 1}: {sqlEx.Message}");
            }
            catch (Exception ex)
            {
                log.WriteLine($"[ERROR] Excepción al registrar {estado} para CEF {cef}, intento {intentos + 1}: {ex.Message}");
            }

            intentos++;

            if (!registrado && intentos < MAX_RETRIES)
            {
                await Task.Delay(2000 * (int)Math.Pow(2, intentos));  // Espera incremental
            }
        }

        if (!registrado)
        {
            log.WriteLine($"[ERROR] No se pudo registrar {estado} para CEF {cef} después de {MAX_RETRIES} intentos.");
        }
    }


    static async Task<bool> VerificarExistenciaRegistros(SqlConnection sqlConnection, string fechaInicio, string fechaFin, string cef, StreamWriter log)
    {
        string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
            WHERE CEF = @CEF AND CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin; ";
            try
        {
            using (var command = new SqlCommand(query, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                int count = (int)await command.ExecuteScalarAsync();
                return count > 0;
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error al verificar la existencia de registros para el CEF {cef}: {ex.Message}");
            return false;
        }
    }


    static async Task EliminarRegistrosEnRango(SqlConnection sqlConnection, string fechaInicio, string fechaFin, StreamWriter log, string cef = null)
    {
        string deleteQuery = @"DELETE FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
            WHERE CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin";

            if (!string.IsNullOrEmpty(cef))
            {
                deleteQuery += " AND CEF = @CEF";
            }

        try
        {
            using (var command = new SqlCommand(deleteQuery, sqlConnection))
            {
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                if (!string.IsNullOrEmpty(cef))
                {
                    command.Parameters.AddWithValue("@CEF", cef);
                }

                int rowsAffected = await command.ExecuteNonQueryAsync();
                log.WriteLine($"Registros eliminados en el rango: {rowsAffected} registros.");
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error al eliminar registros en el rango para el CEF {cef}: {ex.Message}");
        }
    }

    static string ObtenerFecha(string tipo)
    {
        string fecha;
        DateTime fechaValida;
        do
        {
            Console.Write($"Ingrese la fecha de {tipo} (yyyy-MM-dd): ");
            fecha = Console.ReadLine();
        } while (!DateTime.TryParseExact(fecha, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out fechaValida));

        return fecha;
    }

    static string ObtenerCEF()
    {
        Console.Write("Ingrese el CEF que desea consultar (o 'TODOS' para consultar todos): ");
        return Console.ReadLine();
    }

    static bool PreguntarRepetirProceso()
    {
        while (true)
        {
            Console.Write("¿Desea realizar el proceso nuevamente? (S/N): ");
            string respuesta = Console.ReadLine().Trim().ToUpper();

            if (respuesta == "S")
            {
                return true;
            }
            else if (respuesta == "N")
            {
                return false;
            }
            else
            {
                Console.WriteLine("Respuesta no válida. Por favor, ingrese 'S' para Sí o 'N' para No.");
            }
        }
    }

    static (string ipserver, string rutadb, string userdb, string passdb) ObtenerDatosConexion(string cef)
    {
        string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

        try
        {
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                sqlConnection.Open();

                string query = "SELECT ipserver, rutadb, userdb, passdb, region FROM ADM_CEFS WHERE sigla = @cef and activo = 1";
                using (var command = new SqlCommand(query, sqlConnection))
                {
                    command.Parameters.AddWithValue("@cef", cef);

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return (
                                reader["ipserver"].ToString(),
                                reader["rutadb"].ToString(),
                                reader["userdb"].ToString(),
                                reader["passdb"].ToString()
                                );
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // Registrar el error o manejar la excepción según lo necesario
            Console.WriteLine($"Error al obtener datos de conexión para el CEF '{cef}': {ex.Message}");
        }

        return (null, null, null, null);
    }

    static List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> ObtenerTodosLosDatosConexion()
    {
        List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = new List<(string, string, string, string, string)>();

        string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

        try
        {
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                sqlConnection.Open();

                string query = "SELECT sigla, ipserver, rutadb, userdb, passdb, region FROM ADM_CEFS WHERE activo = 1";
                using (var command = new SqlCommand(query, sqlConnection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            listaCefs.Add((
                                reader["sigla"].ToString(),
                                reader["ipserver"].ToString(),
                                reader["rutadb"].ToString(),
                                reader["userdb"].ToString(),
                                reader["passdb"].ToString()
                                ));
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // Registrar el error o manejar la excepción según lo necesario
            Console.WriteLine($"Error al obtener todos los datos de conexión: {ex.Message}");
        }

        return listaCefs;
    }
}s