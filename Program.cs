using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Collections.Generic;

class Program
{
    static void Main(string[] args)
    {
        bool repetirProceso = true;

        while (repetirProceso)
        {
            string fechaInicio = ObtenerFecha("inicio");
            string fechaFin = ObtenerFecha("fin");
            string cef = ObtenerCEF();

            string fechaActual = DateTime.Now.ToString("yyyyMMdd");
            
            // Crear o abrir el archivo de log en la carpeta actual
            string logFilePath = $@"C:\Diniz\Log\log_cargaCEF_{fechaActual}.txt";
            using (StreamWriter log = new StreamWriter(logFilePath, true))
            {
                log.WriteLine($"Proceso iniciado: {DateTime.Now}");

                // Ejecutar el TRUNCATE en ambos casos (cuando es "TODOS" o un CEF específico)
                using (var sqlConnection = new SqlConnection("Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;"))
                {
                    sqlConnection.Open();
                    string deleteQuery = "TRUNCATE TABLE COINTECH_DB.dbo.tickets_db_cointech_cef;";
                    
                    using (var deleteCommand = new SqlCommand(deleteQuery, sqlConnection))
                    {
                        deleteCommand.CommandTimeout = 600;
                        deleteCommand.ExecuteNonQuery();
                        Console.WriteLine("Tabla eliminada correctamente.");
                        log.WriteLine("Tabla eliminada correctamente.");
                    }
                }

                int totalGlobalInsertados = 0; // Llevar el conteo global de registros insertados

                if (cef.ToUpper() == "TODOS")
                {
                    // Consulta todos los CEFs de la tabla ADM_CEFS
                    List<(string sigla, string ipserver, string rutadb, string userdb, string passdb)> listaCefs = ObtenerTodosLosDatosConexion();

                    foreach (var cefDatos in listaCefs)
                    {
                        DateTime inicioCEF = DateTime.Now;
                        Console.WriteLine($"Procesando CEF: {cefDatos.sigla} - Inicio: {inicioCEF}");
                        log.WriteLine($"Procesando CEF: {cefDatos.sigla} - Inicio: {inicioCEF}");

                        // Realizar el proceso para cada CEF
                        int registrosInsertadosCEF = ProcesarCEF(cefDatos.sigla, cefDatos.ipserver, cefDatos.rutadb, cefDatos.userdb, cefDatos.passdb, fechaInicio, fechaFin, log);

                        DateTime finCEF = DateTime.Now;
                        TimeSpan duracionCEF = finCEF - inicioCEF;
                        Console.WriteLine($"Fin del procesamiento para CEF: {cefDatos.sigla} - Fin: {finCEF} - Duración: {duracionCEF.TotalMinutes} minutos");
                        log.WriteLine($"Fin del procesamiento para CEF: {cefDatos.sigla} - Fin: {finCEF} - Duración: {duracionCEF.TotalMinutes} minutos");

                        Console.WriteLine($"Total de registros insertados para el CEF {cefDatos.sigla}: {registrosInsertadosCEF}");
                        log.WriteLine($"Total de registros insertados para el CEF {cefDatos.sigla}: {registrosInsertadosCEF}");
                        
                        Console.WriteLine($"***********************************");
                        log.WriteLine($"***********************************");

                        totalGlobalInsertados += registrosInsertadosCEF;
                    }

                }
                else
                {
                    // Obtener los datos del CEF específico
                    (string ipserver, string rutadb, string userdb, string passdb) = ObtenerDatosConexion(cef);

                    if (string.IsNullOrEmpty(ipserver))
                    {
                        Console.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}'.");
                        log.WriteLine($"No se encontraron datos de conexión para el CEF '{cef}'.");
                        return;
                    }

                    // Realizar el proceso para el CEF específico
                    int registrosInsertadosCEF = ProcesarCEF(cef, ipserver, rutadb, userdb, passdb, fechaInicio, fechaFin, log);

                    // Mostrar solo el total de registros insertados para este CEF
                    
                    log.WriteLine($"Total de registros insertados para el CEF {cef}: {registrosInsertadosCEF}");
                    Console.WriteLine($"***********************************");
                    log.WriteLine($"***********************************");

                    totalGlobalInsertados += registrosInsertadosCEF;
                }

                // Mostrar solo el total global de registros insertados
                log.WriteLine($"######################################");
                log.WriteLine($"Total global de registros insertados: {totalGlobalInsertados}");
                log.WriteLine($"######################################");

                log.WriteLine($"Proceso completado: {DateTime.Now}");
            }

            repetirProceso = PreguntarRepetirProceso();
        }
    }

    // Método para procesar cada CEF
    static int ProcesarCEF(string cef, string ipserver, string rutadb, string userdb, string passdb, string fechaInicio, string fechaFin, StreamWriter log)
    {
        string firebirdConnectionString = $"DRIVER={{Firebird/Interbase(r) driver}};DATABASE={ipserver}/3050:{rutadb};UID={userdb};PWD={passdb};";
        string sqlServerConnectionString = "Server=192.168.0.174;Database=COINTECH_DB;User Id=sa;Password=P@ssw0rd;";

        DateTime fechaInicioDT = DateTime.Parse(fechaInicio);
        DateTime fechaFinDT = DateTime.Parse(fechaFin);
        TimeSpan intervalo = TimeSpan.FromDays(5);

        int totalRegistrosInsertados = 0;
        int totalRegistrosFirebird = 0;

        using (var firebirdConnection = new OdbcConnection(firebirdConnectionString))
        {
            try
            {
                firebirdConnection.ConnectionTimeout = 180; // 3 minutos
                firebirdConnection.Open();
                Console.WriteLine($"***********************************");
                Console.WriteLine($"Conexión exitosa a Firebird para el CEF: {cef}");
                
                log.WriteLine($"***********************************");
                log.WriteLine($"Conexión exitosa a Firebird para el CEF: {cef}");

                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    sqlConnection.Open();

                    while (fechaInicioDT <= fechaFinDT)
                    {
                        DateTime fechaFinBloque = fechaInicioDT.Add(intervalo);
                        if (fechaFinBloque > fechaFinDT)
                        {
                            fechaFinBloque = fechaFinDT;
                        }

                        string query = @"
                            SELECT  
                                b.idtranpos AS id_transaccion,
                                CAST(c.fechayhora AS TIME) AS hora,
                                a.numerocaja - 200 AS numero_terminal,
                                b.tipo,
                                c.numcomprobante AS numero_comprobante,
                                b.importe,
                                CAST(a.fechaadministrativa AS DATE) AS fecha
                            FROM 
                                caj_turnos a
                            INNER JOIN 
                                caj_transacciones b ON a.idturnocaja = b.idturnocaja
                            INNER JOIN 
                                pos_transacciones c ON b.idtranpos = c.idtranpos
                            WHERE 
                                CAST(a.fechaadministrativa AS DATE) BETWEEN ? AND ?
                                AND b.importe <> 0 
                                AND c.numcomprobante > 0;";

                        using (var command = new OdbcCommand(query, firebirdConnection))
                        {
                            command.CommandTimeout = 600;
                            command.Parameters.AddWithValue("@fechaInicio", fechaInicioDT.ToString("yyyy-MM-dd"));
                            command.Parameters.AddWithValue("@fechaFin", fechaFinBloque.ToString("yyyy-MM-dd"));

                            DataTable dataTable = new DataTable();
                            dataTable.Columns.Add("ID_TRANSACCION", typeof(long));
                            dataTable.Columns.Add("HORA", typeof(string));
                            dataTable.Columns.Add("NUMERO_TERMINAL", typeof(int));
                            dataTable.Columns.Add("TIPO", typeof(string));
                            dataTable.Columns.Add("NUMERO_COMPROBANTE", typeof(string));
                            dataTable.Columns.Add("IMPORTE", typeof(decimal));
                            dataTable.Columns.Add("FECHA", typeof(string));
                            dataTable.Columns.Add("CEF", typeof(string));  

                            using (var reader = command.ExecuteReader())
                            {
                                if (!reader.HasRows)
                                {
                                    Console.WriteLine($"No hay registros para el CEF '{cef}' en el rango {fechaInicioDT:yyyy-MM-dd} a {fechaFinBloque:yyyy-MM-dd}.");
                                    log.WriteLine($"No hay registros para el CEF '{cef}' en el rango {fechaInicioDT:yyyy-MM-dd} a {fechaFinBloque:yyyy-MM-dd}.");
                                }

                                while (reader.Read())
                                {
                                    try
                                    {
                                        string horaFormateada = reader["hora"].ToString();
                                        DateTime fecha = (DateTime)reader["fecha"];
                                        string fechaFormateada = fecha.ToString("yyyy-MM-dd");

                                        dataTable.Rows.Add(
                                            reader["id_transaccion"],
                                            horaFormateada,
                                            reader["numero_terminal"],
                                            reader["tipo"],
                                            reader["numero_comprobante"],
                                            reader["importe"],
                                            fechaFormateada,
                                            cef  
                                        );
                                        totalRegistrosFirebird++;
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error al agregar datos al DataTable: {ex.Message}");
                                        log.WriteLine($"Error al agregar datos al DataTable: {ex.Message}");
                                    }
                                }
                            }

                            if (dataTable.Rows.Count > 0)
                            {
                                
                                Console.WriteLine("Se pudo hacer la consulta satisfactoriamente, se procede a insertar datos en SQL Server...");
                                log.WriteLine("Se pudo hacer la consulta satisfactoriamente, se procede a insertar datos en SQL Server...");

                                int batchSize = 50000;

                                for (int i = 0; i < dataTable.Rows.Count; i += batchSize)
                                {
                                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
                                    {
                                        bulkCopy.DestinationTableName = "COINTECH_DB.dbo.tickets_db_cointech_cef";
                                        bulkCopy.BulkCopyTimeout = 600;

                                        bulkCopy.ColumnMappings.Add("ID_TRANSACCION", "ID_TRANSACCION");
                                        bulkCopy.ColumnMappings.Add("HORA", "HORA");
                                        bulkCopy.ColumnMappings.Add("NUMERO_TERMINAL", "NUMERO_TERMINAL");
                                        bulkCopy.ColumnMappings.Add("TIPO", "TIPO");
                                        bulkCopy.ColumnMappings.Add("NUMERO_COMPROBANTE", "NUMERO_COMPROBANTE");
                                        bulkCopy.ColumnMappings.Add("IMPORTE", "IMPORTE");
                                        bulkCopy.ColumnMappings.Add("FECHA", "FECHA");
                                        bulkCopy.ColumnMappings.Add("CEF", "CEF");

                                        DataTable batchTable = dataTable.AsEnumerable().Skip(i).Take(batchSize).CopyToDataTable();

                                        try
                                        {
                                            bulkCopy.WriteToServer(batchTable);
                                            Console.WriteLine($"Lote {i / batchSize + 1} completado.");
                                            log.WriteLine($"Lote {i / batchSize + 1} completado.");
                                            totalRegistrosInsertados += batchTable.Rows.Count;
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error durante la inserción en lote {i / batchSize + 1}: {ex.Message}");
                                            log.WriteLine($"Error durante la inserción en lote {i / batchSize + 1}: {ex.Message}");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("No se insertaron datos en este bloque debido a la falta de registros.");
                                log.WriteLine("No se insertaron datos en este bloque debido a la falta de registros.");
                            }
                        }

                        fechaInicioDT = fechaFinBloque.AddDays(1);
                    }

                    Console.WriteLine($"Proceso completado. Registros insertados en SQL Server: {totalRegistrosInsertados}");
                    log.WriteLine($"Proceso completado. Registros insertados en SQL Server: {totalRegistrosInsertados}");

                    if (totalRegistrosFirebird != totalRegistrosInsertados)
                    {
                        Console.WriteLine($"Advertencia: Los registros obtenidos ({totalRegistrosFirebird}) no coinciden con los insertados ({totalRegistrosInsertados}) en SQL Server.");
                        log.WriteLine($"Advertencia: Los registros obtenidos ({totalRegistrosFirebird}) no coinciden con los insertados ({totalRegistrosInsertados}) en SQL Server.");
                    }
                    else
                    {
                        Console.WriteLine("Los registros coinciden correctamente entre Firebird y SQL Server.");
                        log.WriteLine("Los registros coinciden correctamente entre Firebird y SQL Server.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"No se pudo conectar al CEF {cef}: {ex.Message}");
                log.WriteLine($"No se pudo conectar al CEF {cef}: {ex.Message}");
            }
        }

        return totalRegistrosInsertados;
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

    // Método para obtener los datos de conexión desde SQL Server para un CEF específico
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

    // Método para obtener todos los CEFs si se selecciona "TODOS"
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
