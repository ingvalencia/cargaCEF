using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    private static readonly SemaphoreSlim semaphore = new SemaphoreSlim(5); // Limita a 5 tareas paralelas
    private const int MAX_RETRIES = 2; // Número máximo de reintentos

    static void Main(string[] args)
    {
        bool repetirProceso = true;

        while (repetirProceso)
        {
            // Registrar el tiempo de inicio del proceso
            DateTime inicioProceso = DateTime.Now;

            string fechaInicio = ObtenerFecha("inicio");
            string fechaFin = ObtenerFecha("fin");
            string cef = ObtenerCEF();
            string fechaActual = DateTime.Now.ToString("yyyyMMdd");

            string logFilePath = $@"C:\Diniz\Log\log_cargaCEF_{fechaActual}.txt";
            using (StreamWriter log = new StreamWriter(logFilePath, true))
            {
                // Explicación inicial en el log
                log.WriteLine("************************");
                log.WriteLine("Inicio del Proceso");
                log.WriteLine($"Registro: Proceso iniciado: {inicioProceso}");
                log.WriteLine($"El proceso comienza eliminando los registros existentes en la tabla de destino SQL Server para el CEF \"{cef}\" en el rango de fechas {fechaInicio} a {fechaFin}.");

                int totalGlobalInsertados = 0;
                List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();

                using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
                {
                    sqlConnection.Open();

                    // Verificación y eliminación de registros
                    if (cef.ToUpper() == "TODOS")
                    {
                        List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = ObtenerTodosLosDatosConexion();
                        foreach (var cefDatos in listaCefs)
                        {
                            bool existenRegistros = VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cefDatos.sigla, log).Result;
                            if (existenRegistros)
                            {
                                log.WriteLine($"Registro: Registros existentes para el CEF: {cefDatos.sigla} en el rango de fechas: {fechaInicio} a {fechaFin}. Se eliminarán los registros.");
                                EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cefDatos.sigla).Wait();
                            }
                        }
                    }
                    else
                    {
                        bool existenRegistros = VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cef, log).Result;
                        if (existenRegistros)
                        {
                            log.WriteLine($"Registro: Registros existentes para el CEF: {cef} en el rango de fechas: {fechaInicio} a {fechaFin}. Se eliminarán los registros.");
                            EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cef).Wait();
                        }
                    }
                }

                // Procesamiento de los CEFs
                log.WriteLine("2. Procesamiento de los Bloques de Fechas (de mes en mes)");
                if (cef.ToUpper() == "TODOS")
                {
                    List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = ObtenerTodosLosDatosConexion();

                    if (listaCefs.Count == 0)
                    {
                        log.WriteLine("No se encontraron CEFs activos para procesar.");
                    }
                    else
                    {
                        ProcesarCefs(listaCefs, fechaInicio, fechaFin, log, detallesCefs).Wait();
                    }
                }
                else
                {
                    var (ipserver, rutadb, userdb, passdb) = ObtenerDatosConexion(cef);

                    if (string.IsNullOrEmpty(ipserver))
                    {
                        log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}'.");
                        return;
                    }

                    List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCef = new List<(string, string, string, string, string)>
                    {
                        (cef, ipserver, rutadb, userdb, passdb)
                    };
                    ProcesarCefs(listaCef, fechaInicio, fechaFin, log, detallesCefs).Wait();
                }

                // Resumen del log
                log.WriteLine("************************");
                log.WriteLine("Resumen del proceso de CEFs:");
                double tiempoTotal = 0;
                foreach (var detalle in detallesCefs)
                {
                    log.WriteLine($"CEF: {detalle.cef}, Registros Insertados: {detalle.totalRegistros}, Tiempo: {detalle.tiempoSegundos} segundos");
                    tiempoTotal += detalle.tiempoSegundos;
                    totalGlobalInsertados += detalle.totalRegistros;
                }
                log.WriteLine($"Total global de registros insertados: {totalGlobalInsertados}");

                // Calcular el tiempo total del proceso
                DateTime finProceso = DateTime.Now;
                TimeSpan duracionProceso = finProceso - inicioProceso;
                string tiempoFormateado = $"{duracionProceso.Hours} horas, {duracionProceso.Minutes} minutos, {duracionProceso.Seconds} segundos";

                log.WriteLine($"Tiempo total del proceso: {tiempoFormateado}");
                log.WriteLine($"Proceso completado: {finProceso}");
                log.WriteLine("************************");
            }

            repetirProceso = PreguntarRepetirProceso();
        }
    }

    static async Task ProcesarCefs(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, string fechaInicio, string fechaFin, StreamWriter log, List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs)
    {
        List<Task<int>> tasks = new List<Task<int>>();

        foreach (var cefDatos in listaCefs)
        {
            // Mostrar en consola el CEF que está siendo procesado
            Console.WriteLine($"Procesando CEF: {cefDatos.sigla}...");
            log.WriteLine($"Procesando CEF: {cefDatos.sigla}...");

            tasks.Add(Task.Run(async () =>
            {
                await semaphore.WaitAsync(); // Limita el número de tareas concurrentes
                try
                {
                    DateTime inicioCEF = DateTime.Now;
                    int registrosInsertados = await ProcesarCEFConLogAsync(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin, log);
                    DateTime finCEF = DateTime.Now;
                    double tiempoSegundos = (finCEF - inicioCEF).TotalSeconds;

                    detallesCefs.Add((cefDatos.sigla, registrosInsertados, tiempoSegundos));

                    // Mostrar en consola que terminó de procesar el CEF actual
                    Console.WriteLine($"CEF {cefDatos.sigla} procesado. Registros insertados: {registrosInsertados}. Tiempo: {tiempoSegundos} segundos");
                    log.WriteLine($"CEF {cefDatos.sigla} procesado. Registros insertados: {registrosInsertados}. Tiempo: {tiempoSegundos} segundos");

                    return registrosInsertados;
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }


    static async Task<int> ProcesarCEFConLogAsync(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={ipserver}/3050:{rutadb};UID={userdb};PWD={passdb};";
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int totalRegistrosInsertados = 0;
        int reintentos = 0;
        bool procesoExitoso = false;

        while (reintentos < MAX_RETRIES && !procesoExitoso)
        {
            try
            {
                using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    await firebirdConnection.OpenAsync();
                    await sqlConnection.OpenAsync();
                    log.WriteLine($"Conexión Exitosa a Firebird y SQL Server para el CEF: {cef}");

                    // Inicia una transacción en SQL Server
                    using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction())
                    {
                        DateTime fechaInicioDT = DateTime.Parse(fechaInicio);
                        DateTime fechaFinDT = DateTime.Parse(fechaFin);

                        while (fechaInicioDT <= fechaFinDT)
                        {
                            string fechaActual = fechaInicioDT.ToString("yyyy-MM-dd");
                            log.WriteLine($"Procesando datos para el día: {fechaActual}");

                            try
                            {
                                // Realiza la consulta e inserta los registros
                                int registrosDelDia = await RealizarProcesoDeCargaPorDia(firebirdConnection, sqlConnection, cef, fechaActual, sqlTransaction, log);
                                totalRegistrosInsertados += registrosDelDia;
                                log.WriteLine($"Registros insertados para el día {fechaActual}: {registrosDelDia}");
                            }
                            catch (Exception ex)
                            {
                                // Error en el procesamiento del día
                                log.WriteLine($"Error procesando el día {fechaActual}: {ex.Message}");
                                log.WriteLine("Realizando ROLLBACK del proceso...");
                                sqlTransaction.Rollback();
                                return totalRegistrosInsertados; // Termina el proceso
                            }

                            fechaInicioDT = fechaInicioDT.AddDays(1); // Avanza al siguiente día
                        }

                        // Si todo sale bien, confirmamos la transacción
                        sqlTransaction.Commit();
                        procesoExitoso = true;
                        log.WriteLine("Transacción completada y confirmada exitosamente.");
                    }
                }
            }
            catch (Exception ex)
            {
                reintentos++;
                log.WriteLine($"Error al conectar o procesar CEF {cef}: {ex.Message}");
                log.WriteLine($"Intento de Reintento ({reintentos}/{MAX_RETRIES})...");
                await RegistrarErrorCEF(cef, ex.Message, $"{fechaInicio} - {fechaFin}", log);
            }
        }

        if (!procesoExitoso)
        {
            log.WriteLine($"No se pudo procesar correctamente el CEF {cef} después de {MAX_RETRIES} intentos.");
        }

        return totalRegistrosInsertados;
    }

    static async Task<int> RealizarProcesoDeCargaPorDia(OdbcConnection firebirdConnection, SqlConnection sqlConnection, string cef, string fecha, SqlTransaction transaction, StreamWriter log)
    {
        int batchSize = 10000; // Tamaño del batch para inserciones
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
                log.WriteLine($"Cantidad de registros leídos de Firebird para el día {fecha}: {totalRegistros}");

                // Realiza el insert en SQL Server en lotes
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

                await command.ExecuteNonQueryAsync();
                log.WriteLine($"Error registrado en log_carga_cef para CEF: {cef}.");
            }
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
