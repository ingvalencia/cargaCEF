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

   static async Task Main(string[] args)
    {
        bool repetirProceso = true;

        while (repetirProceso)
        {
            DateTime inicioProcesoGlobal = DateTime.Now;
            List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs = new List<(string, int, double)>();

            string fechaInicio = ObtenerFecha("inicio");
            string fechaFin = ObtenerFecha("fin");
            string cef = ObtenerCEF();
            string fechaActual = DateTime.Now.ToString("yyyyMMdd");

            string logFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_cargaCEF_{fechaActual}.txt";
            using (StreamWriter log = new StreamWriter(logFilePath, true))
            {
                log.WriteLine("*****************************");
                log.WriteLine("Inicio del Proceso");

                // Conexión SQL Server
                using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
                {
                    sqlConnection.Open();

                    if (cef.ToUpper() == "TODOS")
                    {
                        List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = ObtenerTodosLosDatosConexion();
                        foreach (var cefDatos in listaCefs)
                        {
                            // Verificar si hay registros existentes para eliminar
                            bool existenRegistros = await VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cefDatos.sigla, log);
                            if (existenRegistros)
                            {
                                log.WriteLine($"Registro: Registros existentes para el CEF: {cefDatos.sigla} en el rango de fechas: {fechaInicio} a {fechaFin}. Se eliminarán los registros.");
                                await EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cefDatos.sigla);
                            }
                        }

                        if (listaCefs.Count == 0)
                        {
                            log.WriteLine("No se encontraron CEFs activos para procesar.");
                        }
                        else
                        {
                            // Procesar CEFs en grupos
                            await ProcesarGruposDeCefs(listaCefs, fechaInicio, fechaFin, log);

                            // Llamamos a VerificarRegistrosCefs después de procesar los CEFs
                            await VerificarRegistrosCefs(listaCefs, fechaInicio, fechaFin);
                        }
                    }
                    else
                    {
                        bool existenRegistros = await VerificarExistenciaRegistros(sqlConnection, fechaInicio, fechaFin, cef, log);
                        if (existenRegistros)
                        {
                            log.WriteLine($"Registro: Registros existentes para el CEF: {cef} en el rango de fechas: {fechaInicio} a {fechaFin}. Se eliminarán los registros.");
                            await EliminarRegistrosEnRango(sqlConnection, fechaInicio, fechaFin, log, cef);
                        }

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
                        
                        // Procesar CEF individual
                        await ProcesarGruposDeCefs(listaCef, fechaInicio, fechaFin, log);

                        // Llamamos a VerificarRegistrosCefs después de procesar el CEF individual
                        await VerificarRegistrosCefs(listaCef, fechaInicio, fechaFin);
                    }
                }

                // Resumen global al final del proceso
                DateTime finProcesoGlobal = DateTime.Now;
                TimeSpan duracionProcesoGlobal = finProcesoGlobal - inicioProcesoGlobal;
                string tiempoFormateadoGlobal = $"{duracionProcesoGlobal.Hours} horas, {duracionProcesoGlobal.Minutes} minutos, {duracionProcesoGlobal.Seconds} segundos";

                log.WriteLine("#############################");
                log.WriteLine("Tiempo total en que se tardó todo el proceso en ejecutarse");
                log.WriteLine($"Inicio del procesamiento: {inicioProcesoGlobal.ToString("yyyy-MM-dd HH:mm:ss")}");
                log.WriteLine($"Fin del procesamiento total: {finProcesoGlobal.ToString("yyyy-MM-dd HH:mm:ss")}");
                log.WriteLine($"Tiempo total del procesamiento: {tiempoFormateadoGlobal}");
                log.WriteLine("#############################");
            }

            repetirProceso = PreguntarRepetirProceso();
        }
    }


    static List<List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)>> DividirEnGrupos(
    List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, int tamanoGrupo)
    {
        var grupos = new List<List<(string, string, string, string, string)>>();

        for (int i = 0; i < listaCefs.Count; i += tamanoGrupo)
        {
            grupos.Add(listaCefs.GetRange(i, Math.Min(tamanoGrupo, listaCefs.Count - i)));
        }

        return grupos;
    }

    static async Task ProcesarGruposDeCefs(
    List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, 
    string fechaInicio, 
    string fechaFin, 
    StreamWriter log)
    {
        int tamanoGrupo = 5;  // Puedes ajustar el tamaño del grupo

        List<List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)>> grupos = DividirEnGrupos(listaCefs, tamanoGrupo);

        foreach (var grupo in grupos)
        {
            log.WriteLine($"Iniciando procesamiento del grupo de CEFs (Tamaño: {grupo.Count})");

            // Procesar el grupo en paralelo (usa la función ProcesarCefs que ya tienes)
            await ProcesarCefs(grupo, fechaInicio, fechaFin, log, new List<(string, int, double)>());

            log.WriteLine("Finalizado procesamiento del grupo.");
        }
    }



    static async Task VerificarRegistrosCefs(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, string fechaInicio, string fechaFin)
    {
        string logFilePath = $@"C:\Users\Administrador\Documents\Proyectos_Gio\log_verificacion_ticket_{DateTime.Now.ToString("yyyyMMdd")}.txt";

        using (StreamWriter log = new StreamWriter(logFilePath, true))
        {
            foreach (var cefDatos in listaCefs)
            {
                // Obtener conteo de SQL Server
                int totalSqlServer = await ObtenerConteoSqlServer(cefDatos.sigla, fechaInicio, fechaFin);
                // Obtener conteo de Firebird
                int totalFirebird = await ObtenerConteoFirebird(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin);

                // Registrar en el log
                log.WriteLine("*****************************");
                log.WriteLine($"CEF {cefDatos.sigla}: consultados en SQL Server: {totalSqlServer} - consultados en Firebird: {totalFirebird}");

                if (totalSqlServer != totalFirebird)
                {
                    log.WriteLine($"Discrepancia detectada en el CEF {cefDatos.sigla}: SQL Server: {totalSqlServer}, Firebird: {totalFirebird}");
                }
                log.WriteLine("*****************************");
            }
        }
    }

    // Método para obtener conteo en SQL Server
    static async Task<int> ObtenerConteoSqlServer(string cef, string fechaInicio, string fechaFin)
    {
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";
        int totalRegistros = 0;

        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            await sqlConnection.OpenAsync();

            string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.MEM 
                            WHERE CEF = @CEF AND CAST(FECHA AS DATE) BETWEEN @fechaInicio AND @fechaFin;";

            using (var command = new SqlCommand(query, sqlConnection))
            {
                command.Parameters.AddWithValue("@CEF", cef);
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);

                totalRegistros = (int)await command.ExecuteScalarAsync();
            }
        }

        return totalRegistros;
    }

    // Método para obtener conteo en Firebird
    static async Task<int> ObtenerConteoFirebird(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin)
    {
        int totalRegistros = 0;

        string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={ipserver}/3050:{rutadb};UID={userdb};PWD={passdb};";

        using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
        {
            await firebirdConnection.OpenAsync();

            string query = @"SELECT COUNT(*) 
                            FROM caj_turnos a
                            INNER JOIN caj_transacciones b ON a.idturnocaja = b.idturnocaja
                            INNER JOIN pos_transacciones c ON b.idtranpos = c.idtranpos
                            WHERE CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ? 
                            AND b.importe <> 0 AND c.numcomprobante > 0;";

            using (var command = new OdbcCommand(query, firebirdConnection))
            {
                command.Parameters.AddWithValue("@fechaInicio", fechaInicio);
                command.Parameters.AddWithValue("@fechaFin", fechaFin);
                command.Parameters.AddWithValue("@cef", cef);

                totalRegistros = (int)await command.ExecuteScalarAsync();
            }
        }

        return totalRegistros;
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
        int intentos = 0;
        bool exito = false;

        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        while (intentos < MAX_RETRIES && !exito)
        {
            intentos++;
            log.WriteLine($"Intento {intentos} para el CEF {cef}");

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

                exito = true; // Si llega aquí, significa que el proceso fue exitoso
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error procesando CEF {cef} en reintento {intentos}: {ex.Message}");

                // Intentar registrar el error en log_carga_cef
                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    await sqlConnection.OpenAsync();
                    await RegistrarErrorCEF(cef, ex.Message, fechaRango, log, sqlConnection);
                }
            }
        }

        // Crear conexión SQL Server para registro final
        using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
        {
            await sqlConnection.OpenAsync();

            if (exito)
            {
                // Si el CEF se procesó correctamente, eliminar el registro de log_carga_cef
                await EliminarRegistroLogCEF(sqlConnection, cef, fechaRango);
            }
            else
            {
                // Si después de 2 intentos sigue fallando, actualizar el estado a "fallido"
                log.WriteLine($"CEF {cef} falló después de {MAX_RETRIES} intentos. No se volverá a intentar.");
                await ActualizarRegistroLogCEF(sqlConnection, cef, fechaRango);
            }
        }

        return exito;
    }

    static async Task RegistrarErrorCEF(string cef, string error, string fechaRango, StreamWriter log, SqlConnection sqlConnection)
    {
        string insertQuery = @"INSERT INTO log_carga_cef (CEF, tipoError, fechaRango, fechaRegistro, estado, reintentos) 
                            VALUES (@CEF, @tipoError, @fechaRango, @fechaRegistro, 'pendiente', 0)";

        try
        {
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
        catch (Exception ex)
        {
            log.WriteLine($"Error al intentar registrar en log_carga_cef: {ex.Message}");
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


    static async Task ProcesarCefs(List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs, string fechaInicio, string fechaFin, StreamWriter log, List<(string cef, int totalRegistros, double tiempoSegundos)> detallesCefs)
    {
        List<Task> tasks = new List<Task>();

        foreach (var cefDatos in listaCefs)
        {
            tasks.Add(Task.Run(async () =>
            {
                Console.WriteLine($"Procesando CEF: {cefDatos.sigla}..."); // Mostrar en consola que comienza el procesamiento del CEF

                DateTime inicioCEF = DateTime.Now;
                log.WriteLine("*****************************");
                log.WriteLine($"CEF: {cefDatos.sigla}");
                log.WriteLine($"Inicio del procesamiento: {inicioCEF.ToString("yyyy-MM-dd HH:mm:ss")}");

                int totalRegistrosInsertados = 0;
                bool fallo = false;
                string mensajeFallo = string.Empty;

                try
                {
                    totalRegistrosInsertados = await ProcesarCEFConLogAsync(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin, log);
                }
                catch (Exception ex)
                {
                    fallo = true;
                    mensajeFallo = $"Fallo: {ex.Message}";
                }

                DateTime finCEF = DateTime.Now;
                double tiempoSegundos = (finCEF - inicioCEF).TotalSeconds;

                // Formateo del tiempo total de procesamiento
                TimeSpan duracionProceso = finCEF - inicioCEF;
                string tiempoFormateado = $"{duracionProceso.Hours} horas, {duracionProceso.Minutes} minutos, {duracionProceso.Seconds} segundos";

                if (fallo)
                {
                    log.WriteLine($"Status: {mensajeFallo}");
                }
                else
                {
                    log.WriteLine($"Status: No presentó problemas de inserción, se registraron: {totalRegistrosInsertados} registros");
                }

                log.WriteLine($"Fin del procesamiento: {finCEF.ToString("yyyy-MM-dd HH:mm:ss")}");
                log.WriteLine($"Tiempo total de procesamiento: {tiempoFormateado}");
                log.WriteLine("*****************************");

                // Mostrar en consola que finalizó el procesamiento del CEF
                Console.WriteLine($"CEF {cefDatos.sigla} procesado.");
                Console.WriteLine($"Tiempo total del procesamiento del CEF {cefDatos.sigla}: {tiempoFormateado}");

                detallesCefs.Add((cefDatos.sigla, totalRegistrosInsertados, tiempoSegundos));
            }));
        }

        await Task.WhenAll(tasks);
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
                            bulkCopy.DestinationTableName = "COINTECH_DB.dbo.MEM";
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
        string query = @"SELECT COUNT(*) FROM COINTECH_DB.dbo.MEM 
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
        string deleteQuery = @"DELETE FROM COINTECH_DB.dbo.MEM 
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
