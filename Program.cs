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

   static async Task Main(string[] args)
    {
        bool repetirProceso = true;

        while (repetirProceso)
        {
            int opcion = MostrarMenu(); // Mostrar el menú y obtener la opción del usuario

            // Variables comunes
            DateTime inicioProcesoGlobal = DateTime.Now;
            List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefsProcesados = new List<(string, string, string, string, string)>();
            List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();
            string fechaInicio = ObtenerFecha("inicio");
            string fechaFin = ObtenerFecha("fin");
            List<string> cefsList = ObtenerListaCefs(); // Ajuste para lista de CEFs
            string fechaActual = DateTime.Now.ToString("yyyyMMdd");

            // Variable para acumular el tiempo total de procesamiento de todos los CEFs
            TimeSpan tiempoTotalCefs = TimeSpan.Zero;

            // Abrir el log para registros
            string logFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_cargaCEF_{fechaActual}.txt";
            using (StreamWriter log = new StreamWriter(logFilePath, true))
            {
                // Opción 1: Realizar proceso de carga de CEFs completo
                if (opcion == 1)
                {
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
                            listaCefsProcesados = listaCefs; // Guardamos la lista de CEFs procesados

                            foreach (var cefDatos in listaCefs)
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
                            foreach (string cef in cefsList)
                            {
                                // Obtener datos de conexión para cada CEF
                                var (ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);

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

                    // Nuevo: Verificación de conteo por CEF
                    Console.WriteLine("Proceso completado, ahora se procede con hacer un conteo por cada CEF.");
                    string logVerificationFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_verificacion_ticket_{fechaActual}.txt";
                    await VerificarConteoRegistrosPorCEF(listaCefsProcesados, fechaInicio, fechaFin, logVerificationFilePath);
                }
                // Opción 2: Realizar solo el conteo de registros por CEF
                else if (opcion == 2)
                {
                    Console.WriteLine("Verificación de Conteo de Registros por CEF:");
                    string logVerificationFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_verificacion_ticket_{fechaActual}.txt";

                    // Rellenar listaCefsProcesados con la información de conexión de los CEFs seleccionados
                    if (cefsList.Count == 1 && cefsList[0].ToUpper() == "TODOS")
                    {
                        // Procesar todos los CEFs
                        listaCefsProcesados = ObtenerTodosLosDatosConexion();
                    }
                    else
                    {
                        foreach (string cef in cefsList)
                        {
                            // Obtener datos de conexión para cada CEF
                            var (ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);
                            if (!string.IsNullOrEmpty(ipserver))
                            {
                                listaCefsProcesados.Add((cef, ipserver, rutadb, userdb, passdb));
                            }
                        }
                    }

                    // Verificar el conteo de registros con la lista de CEFs procesados
                    await VerificarConteoRegistrosPorCEF(listaCefsProcesados, fechaInicio, fechaFin, logVerificationFilePath);
                }
            }

            repetirProceso = PreguntarRepetirProceso();
        }
    }

    // Método para mostrar el menú de opciones
    static int MostrarMenu()
    {
        Console.WriteLine("Seleccione una opción:");
        Console.WriteLine("1. Realizar Proceso de Carga CEF Completo");
        Console.WriteLine("2. Verificar Conteo de Registros por CEF");
        
        int opcion;
        do
        {
            Console.Write("Ingrese su opción (1 o 2): ");
        } while (!int.TryParse(Console.ReadLine(), out opcion) || (opcion != 1 && opcion != 2));

        return opcion;
    }

    // Método para obtener lista de CEFs
    static List<string> ObtenerListaCefs()
    {
        Console.Write("Ingrese el/los CEF(s) que desea consultar (separados por comas si son múltiples, o 'TODOS' para consultar todos): ");
        string input = Console.ReadLine();
        
        if (input.ToUpper().Trim() == "TODOS")
            return new List<string> { "TODOS" };
        
        return input.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(cef => cef.Trim().ToUpper())
                    .ToList();
    }

    // Método de reintentos con timeout
    static async Task<int> ProcesarCEFConReintentos(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        int intento = 0;
        int totalRegistrosInsertados = 0;

        while (intento < MAX_RETRIES)
        {
            try
            {
                intento++;
                log.WriteLine($"Intento {intento} de {MAX_RETRIES} para procesar CEF {cef}");
                
                // Procesar el CEF
                totalRegistrosInsertados = await ProcesarCEFConLogAsync(cef, ipserver, rutadb, userdb, passdb, fechaInicio, fechaFin, log);
                
                // Si el proceso fue exitoso, salir del bucle
                return totalRegistrosInsertados;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error en el intento {intento} para CEF {cef}: {ex.Message}");
                
                // Si es el último intento, registrar como fallo
                if (intento >= MAX_RETRIES)
                {
                    log.WriteLine($"Fallo al procesar CEF {cef} después de {MAX_RETRIES} intentos.");
                    break;
                }
            }
        }

        return totalRegistrosInsertados;
    }

    // Método para verificar el conteo de registros por CEF mes a mes
    static async Task VerificarConteoRegistrosPorCEF(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefsProcesados, string fechaInicio, string fechaFin, string logVerificationFilePath)
    {
        // Abre el archivo de log para agregar la verificación de cada CEF
        using (StreamWriter logVerification = new StreamWriter(logVerificationFilePath, true))
        {
            // Registra el tiempo de inicio del proceso de verificación
            logVerification.WriteLine($"Proceso de verificación iniciado: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
            
            foreach (var cefDatos in listaCefsProcesados)
            {
                // Tiempo de inicio para el CEF actual
                DateTime inicioCEF = DateTime.Now;
                logVerification.WriteLine("*****************************");
                logVerification.WriteLine($"CEF {cefDatos.sigla}:");

                DateTime fechaInicioDT = DateTime.ParseExact(fechaInicio, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                DateTime fechaFinDT = DateTime.ParseExact(fechaFin, "yyyy-MM-dd", CultureInfo.InvariantCulture);

                // Iterar por cada mes en el rango de fechas
                while (fechaInicioDT <= fechaFinDT)
                {
                    // Obtener el inicio y el fin del mes actual
                    DateTime inicioMes = new DateTime(fechaInicioDT.Year, fechaInicioDT.Month, 1);
                    DateTime finMes = inicioMes.AddMonths(1).AddDays(-1);

                    // Asegurar que no pase el rango máximo establecido por fechaFin
                    if (finMes > fechaFinDT) finMes = fechaFinDT;

                    logVerification.WriteLine($"## Mes: {inicioMes.ToString("MMMM yyyy")} ##");
                    logVerification.WriteLine($"Rango de consulta: {inicioMes.ToString("yyyy-MM-dd")} a {finMes.ToString("yyyy-MM-dd")}");

                    try
                    {
                        // Obtener conteo de registros en SQL Server
                        int countSqlServer = await ObtenerConteoSqlServer(cefDatos.sigla, inicioMes.ToString("yyyy-MM-dd"), finMes.ToString("yyyy-MM-dd"));

                        // Obtener conteo de registros en Firebird
                        int countFirebird = await ObtenerConteoFirebird(cefDatos, inicioMes.ToString("yyyy-MM-dd"), finMes.ToString("yyyy-MM-dd"));

                        // Registrar el conteo en el log
                        logVerification.WriteLine($"Registros consultados en SQL Server: {countSqlServer}");
                        logVerification.WriteLine($"Registros consultados en Firebird: {countFirebird}");
                    }
                    catch (Exception ex)
                    {
                        // Si hay un error, registrarlo en el log y continuar con el siguiente mes
                        logVerification.WriteLine($"No se pudo hacer la consulta para el mes {inicioMes.ToString("MMMM yyyy")} debido a un Error: {ex.Message}");
                        logVerification.WriteLine("Continuando con el siguiente mes...");
                    }

                    // Avanzar al siguiente mes
                    fechaInicioDT = fechaInicioDT.AddMonths(1);
                }

                // Tiempo de finalización para el CEF actual
                DateTime finCEF = DateTime.Now;
                TimeSpan duracionCEF = finCEF - inicioCEF;
                string tiempoFormateadoCEF = $"{duracionCEF.Hours} horas, {duracionCEF.Minutes} minutos, {duracionCEF.Seconds} segundos";

                // Registrar el tiempo de consulta para el CEF en el log
                logVerification.WriteLine($"Inicio de la consulta: {inicioCEF.ToString("yyyy-MM-dd HH:mm:ss")}");
                logVerification.WriteLine($"Fin de la consulta: {finCEF.ToString("yyyy-MM-dd HH:mm:ss")}");
                logVerification.WriteLine($"Tiempo total de consulta para el CEF {cefDatos.sigla}: {tiempoFormateadoCEF}");
                logVerification.WriteLine("*****************************");
            }

            // Registra el tiempo de finalización del proceso de verificación
            logVerification.WriteLine($"Proceso de verificación finalizado: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
        }
    }



    static async Task<int> ObtenerConteoSqlServer(string cef, string fechaInicio, string fechaFin)
    {
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int count = 0;

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            await sqlConnection.OpenAsync();

            string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
                             WHERE CEF = @CEF AND CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin;";

            using (var command = new SqlCommand(query, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                count = (int)await command.ExecuteScalarAsync();
            }
        }

        return count;
    }

    static async Task<int> ObtenerConteoFirebird((string sigla, string ipserver, string rutadb, string userdb, string passdb) cefDatos, string fechaInicio, string fechaFin)
    {
        return await Task.Run(() =>
        {
            string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={cefDatos.ipserver}/3050:{cefDatos.rutadb};UID={cefDatos.userdb};PWD={cefDatos.passdb};";
            int count = 0;

            using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
            {
                firebirdConnection.Open();

                string query = @"SELECT COUNT(*) FROM caj_turnos a
                                 INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                                 INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                                 WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                                 AND b.importe <> 0 AND c.numcomprobante > 0;";

                using (var command = new OdbcCommand(query, firebirdConnection))
                {
                    command.CommandTimeout = 600;
                    command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                    command.Parameters.AddWithValue("@fechaFin", fechaFin);

                    var result = command.ExecuteScalar();
                    if (result != null && int.TryParse(result.ToString(), out int countResult))
                    {
                        count = countResult;
                    }
                    else
                    {
                        count = 0;
                    }
                }
            }

            return count;
        });
    }


    // Nuevo: Método para reintentar los CEFs que fallaron en el proceso anterior
    static async Task ReintentarCefsFallidos(StreamWriter log)
    {
        using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
        {
            sqlConnection.Open();
            string query = "SELECT CEF, fechaRango FROM log_carga_cef WHERE estado = 'pendiente'";
            using (var command = new SqlCommand(query, sqlConnection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    List<(string cef, string fechaRango)> cefsFallidos = new List<(string cef, string fechaRango)>();

                    while (reader.Read())
                    {
                        cefsFallidos.Add((reader["CEF"].ToString(), reader["fechaRango"].ToString()));
                    }

                    foreach (var (cef, fechaRango) in cefsFallidos)
                    {
                        log.WriteLine($"Reintentando CEF fallido: {cef} en el rango {fechaRango}");

                        // Intentar procesar el CEF nuevamente
                        bool exito = await ReprocesarCEF(cef, fechaRango, log);

                        if (exito)
                        {
                            log.WriteLine($"CEF {cef} procesado correctamente. Eliminando de log_carga_cef.");
                            await EliminarRegistroLogCEF(sqlConnection, cef, fechaRango);
                        }
                        else
                        {
                            log.WriteLine($"CEF {cef} falló nuevamente en el reintento. No se volverá a intentar.");
                            await ActualizarRegistroLogCEF(sqlConnection, cef, fechaRango);
                        }
                    }
                }
            }
        }
    }

    static async Task<bool> ReprocesarCEF(string cef, string fechaRango, StreamWriter log)
    {
        try
        {
            var (ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);
            if (string.IsNullOrEmpty(ipserver))
            {
                log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}' durante el reintento.");
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

            List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();

            await ProcesarCefs(listaCef, fechaInicio, fechaFin, log, detallesCefs);

            return true;
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error procesando CEF {cef} en reintento: {ex.Message}");
            return false;
        }
    }

    static async Task EliminarRegistroLogCEF(SqlConnection sqlConnection, string cef, string fechaRango)
    {
        string deleteQuery = "DELETE FROM log_carga_cef WHERE CEF = @CEF AND fechaRango = @fechaRango";
        using (var command = new SqlCommand(deleteQuery, sqlConnection))
        {
            command.Parameters.AddWithValue("@CEF", cef);
            command.Parameters.AddWithValue("@fechaRango", fechaRango);
            await command.ExecuteNonQueryAsync();
        }
    }

    static async Task ActualizarRegistroLogCEF(SqlConnection sqlConnection, string cef, string fechaRango)
    {
        string updateQuery = "UPDATE log_carga_cef SET estado = 'fallido' WHERE CEF = @CEF AND fechaRango = @fechaRango";
        using (var command = new SqlCommand(updateQuery, sqlConnection))
        {
            command.Parameters.AddWithValue("@CEF", cef);
            command.Parameters.AddWithValue("@fechaRango", fechaRango);
            await command.ExecuteNonQueryAsync();
        }
    }

    static async Task<TimeSpan> ProcesarCefs(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, string fechaInicio, string fechaFin, StreamWriter log, List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs)
    {
        TimeSpan tiempoTotal = TimeSpan.Zero; // Variable para acumular el tiempo total de procesamiento de los CEFs

        foreach (var cefDatos in listaCefs)
        {
            // Mostrar en la consola el CEF que se está procesando
            Console.WriteLine($"Procesando CEF: {cefDatos.sigla}...");

            // Iniciar el procesamiento de un solo CEF y registrar el tiempo
            DateTime inicioCEF = DateTime.Now;
            log.WriteLine("*****************************");
            log.WriteLine($"CEF: {cefDatos.sigla}");
            log.WriteLine($"Inicio del procesamiento: {inicioCEF.ToString("yyyy-MM-dd HH:mm:ss")}");

            int totalRegistrosInsertados = 0;
            bool fallo = false;
            string mensajeFallo = string.Empty;

            try
            {
                // Procesa el CEF y almacena los registros insertados
                totalRegistrosInsertados = await ProcesarCEFConLogAsync(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin, log);
            }
            catch (Exception ex)
            {
                fallo = true;
                mensajeFallo = $"Fallo: {ex.Message}";
            }

            DateTime finCEF = DateTime.Now;
            TimeSpan duracionProceso = finCEF - inicioCEF;

            // Sumar el tiempo de procesamiento de este CEF al acumulador
            tiempoTotal += duracionProceso;

            string tiempoFormateado = $"{duracionProceso.Hours} horas, {duracionProceso.Minutes} minutos, {duracionProceso.Seconds} segundos";

            if (fallo)
            {
                log.WriteLine($"Status: {mensajeFallo}");
            }
            else
            {
                log.WriteLine($"Status: No presentó problemas de inserción, se registraron: {totalRegistrosInsertados} registros");
            }

            log.WriteLine($"Fin del procesamiento para CEF {cefDatos.sigla}: {finCEF.ToString("yyyy-MM-dd HH:mm:ss")}");
            log.WriteLine($"Tiempo total del procesamiento para CEF {cefDatos.sigla}: {tiempoFormateado}");
            log.WriteLine("*****************************");

            // Mostrar en consola que finalizó el procesamiento del CEF
            Console.WriteLine($"CEF {cefDatos.sigla} procesado.");
            Console.WriteLine($"Tiempo total del procesamiento del CEF {cefDatos.sigla}: {tiempoFormateado}");

            // Guardar detalles para el resumen final
            detallesCefs.Add((cefDatos.sigla, totalRegistrosInsertados, duracionProceso.TotalSeconds));
        }

        // Retornar el tiempo total acumulado de los CEFs procesados
        return tiempoTotal;
    }


    static async Task<int> ProcesarCEFConLogAsync(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={ipserver}/3050:{rutadb};UID={userdb};PWD={passdb};";
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int totalRegistrosInsertados = 0;

        try
        {
            using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await firebirdConnection.OpenAsync();
                await sqlConnection.OpenAsync();
                
                // Eliminar este mensaje del log si no se necesita mostrar la conexión exitosa
                // log.WriteLine($"Conexión Exitosa a Firebird y SQL Server para el CEF: {cef}");

                using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction())
                {
                    DateTime fechaInicioDT = DateTime.Parse(fechaInicio);
                    DateTime fechaFinDT = DateTime.Parse(fechaFin);

                    while (fechaInicioDT <= fechaFinDT)
                    {
                        string fechaActual = fechaInicioDT.ToString("yyyy-MM-dd");

                        try
                        {
                            int registrosDelDia = await RealizarProcesoDeCargaPorDia(firebirdConnection, sqlConnection, cef, fechaActual, sqlTransaction, log);
                            totalRegistrosInsertados += registrosDelDia;

                            // Eliminar o comentar las siguientes líneas que escriben en el log detalles día por día:
                            // log.WriteLine($"Procesando datos para el día: {fechaActual}");
                            // log.WriteLine($"Cantidad de registros leídos de Firebird para el día {fechaActual}: {totalRegistros}");
                            // log.WriteLine($"Registros insertados para el día {fechaActual}: {registrosDelDia}");
                        }
                        catch (Exception ex)
                        {
                            // Si hay un error, hacemos rollback y lanzamos la excepción para registrar el fallo global del CEF
                            log.WriteLine("Realizando ROLLBACK del proceso...");
                            sqlTransaction.Rollback();
                            throw new Exception($"Error procesando CEF {cef} en el día {fechaActual}: {ex.Message}");
                        }

                        fechaInicioDT = fechaInicioDT.AddDays(1);
                    }

                    sqlTransaction.Commit();
                    // Confirmar éxito al finalizar todo el procesamiento del CEF
                    // log.WriteLine("Transacción completada y confirmada exitosamente.");
                }
            }
        }
        catch (Exception ex)
        {
            // Si ocurre algún error, se lanza una excepción para que sea capturado y registrado en el log final del CEF
            log.WriteLine($"Error al procesar el CEF {cef}: {ex.Message}");
            throw;
        }

        return totalRegistrosInsertados;
    }

    static async Task<int> RealizarProcesoDeCargaPorDia(OdbcConnection firebirdConnection, SqlConnection sqlConnection, string cef, string fecha, SqlTransaction transaction, StreamWriter log)
    {
        int batchSize = 10000;
        int totalRegistrosInsertados = 0;

        string query = @"SELECT 
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
                    CAST(a.fechaadministrativa AS DATE) = ? 
                    AND b.importe <> 0 
                    AND c.numcomprobante > 0;";

        using (var command = new OdbcCommand(query, firebirdConnection))
        {
            command.CommandTimeout = 600;
            command.Parameters.AddWithValue("@fechaActual", fecha);

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
                dataTable.Columns.Add("CEF", typeof(string));

                while (await reader.ReadAsync())
                {
                    dataTable.Rows.Add(
                        reader["id_transaccion"],
                        reader["hora"].ToString(),
                        reader["numero_terminal"],
                        reader["tipo"],
                        reader["numero_comprobante"],
                        reader["importe"],
                        fecha,
                        cef
                    );
                }

                int totalRegistros = dataTable.Rows.Count;
                //log.WriteLine($"Cantidad de registros leídos de Firebird para el día {fecha}: {totalRegistros}");

                if (totalRegistros > 0)
                {
                    for (int i = 0; i < totalRegistros; i += batchSize)
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.TableLock, transaction))
                        {
                            bulkCopy.DestinationTableName = "COINTECH_DB.dbo.tickets_db_cointech_cef";
                            bulkCopy.BulkCopyTimeout = 600;
                            bulkCopy.BatchSize = batchSize;

                            bulkCopy.ColumnMappings.Add("ID_TRANSACCION", "ID_TRANSACCION");
                            bulkCopy.ColumnMappings.Add("HORA", "HORA");
                            bulkCopy.ColumnMappings.Add("NUMERO_TERMINAL", "NUMERO_TERMINAL");
                            bulkCopy.ColumnMappings.Add("TIPO", "TIPO");
                            bulkCopy.ColumnMappings.Add("NUMERO_COMPROBANTE", "NUMERO_COMPROBANTE");
                            bulkCopy.ColumnMappings.Add("IMPORTE", "IMPORTE");
                            bulkCopy.ColumnMappings.Add("FECHA", "FECHA");
                            bulkCopy.ColumnMappings.Add("CEF", "CEF");

                            DataTable batchTable = dataTable.AsEnumerable().Skip(i).Take(batchSize).CopyToDataTable();
                            await bulkCopy.WriteToServerAsync(batchTable);
                            totalRegistrosInsertados += batchTable.Rows.Count;
                        }
                    }
                }
            }
        }

        return totalRegistrosInsertados;
    }

    static async Task RegistrarErrorCEF(string cef, string error, string fechaRango, StreamWriter log)
    {
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        try
        {
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                await sqlConnection.OpenAsync();
                string insertQuery = @"INSERT INTO log_carga_cef (CEF, tipoError, fechaRango, fechaRegistro, estado, reintentos) 
                                    VALUES (@CEF, @tipoError, @fechaRango, @fechaRegistro, 'pendiente', 0)";

                using (var command = new SqlCommand(insertQuery, sqlConnection))
                {
                    command.Parameters.AddWithValue("@CEF", cef);
                    command.Parameters.AddWithValue("@tipoError", error);
                    command.Parameters.AddWithValue("@fechaRango", fechaRango);
                    command.Parameters.AddWithValue("@fechaRegistro", DateTime.Now);

                    log.WriteLine($"Intentando registrar error en log_carga_cef para CEF: {cef}...");

                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    
                    // Confirmar si la inserción fue exitosa
                    if (rowsAffected > 0)
                    {
                        log.WriteLine($"Error registrado correctamente en log_carga_cef para CEF: {cef}.");
                    }
                    else
                    {
                        log.WriteLine($"Error: No se pudo insertar el error en log_carga_cef para CEF: {cef}.");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            log.WriteLine($"Error al intentar registrar en log_carga_cef: {ex.Message}");
        }
    }


    static async Task<bool> VerificarExistenciaRegistros(SqlConnection sqlConnection, string fechaInicio, string fechaFin, string cef, StreamWriter log)
    {
        string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.tickets_db_cointech_cef 
                         WHERE CEF = @CEF AND CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin;";

        using (var command = new SqlCommand(query, sqlConnection))
        {
            command.Parameters.AddWithValue("@CEF", cef);
            command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
            command.Parameters.AddWithValue("@fechaFin", fechaFin);

            int count = (int)await command.ExecuteScalarAsync();
            return count > 0;
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

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            sqlConnection.Open();

            string query = "SELECT ipserver, rutadb, userdb, passdb FROM ADM_CEFS WHERE sigla = @cef and activo = 1";
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

        return (null, null, null, null);
    }

    static List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> ObtenerTodosLosDatosConexion()
    {
        List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = new List<(string, string, string, string, string)>();

        string sqlServerConnectionString = "Server=192.168.0.174;Database=DWBI;User Id=sa;Password=P@ssw0rd;";

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            sqlConnection.Open();

            string query = "SELECT sigla, ipserver, rutadb, userdb, passdb FROM ADM_CEFS WHERE activo = 1";
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

        return listaCefs;
    }
}
