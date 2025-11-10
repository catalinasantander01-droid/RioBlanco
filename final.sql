-- TABLA PROVEEDORES
CREATE TABLE Proveedores (
    Id_ProveedorAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Proveedor VARCHAR(50),
    Nombre VARCHAR(100),
    Telefono VARCHAR(50),
    Correo VARCHAR(100),
    Ciudad VARCHAR(100)
);

-- TABLA CATEGORIAS
CREATE TABLE Categorias (
    Id_CategoriaAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Categoria VARCHAR(50),
    Nombre VARCHAR(100),
    Especie VARCHAR(100)
);

-- TABLA PRODUCTOS
CREATE TABLE Productos (
    Id_ProductoAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Producto VARCHAR(50),
	Id_Proveedor VARCHAR(50),
    Nombre VARCHAR(100),
    Tipo VARCHAR(100),
    Variedad VARCHAR(100),
    Estado_Madurez VARCHAR(100),
    Especie VARCHAR(100),
    Calibre VARCHAR(50),
    Codigo_Embalaje VARCHAR(50),
    N_Caja VARCHAR(50),
    Categoria VARCHAR(100),
    Tipo_Bin VARCHAR(100),
    Id_ProveedorAU INT,
    FOREIGN KEY (Id_ProveedorAU) REFERENCES Proveedores(Id_ProveedorAU)
);

-- TABLA PROCESOS
CREATE TABLE Procesos (
    Id_ProcesoAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Proceso VARCHAR(50),
	Id_Producto VARCHAR(50),
    Id_ProductoAU INT,
    Cantidad_Recepcion VARCHAR(1000),
    Cantidad_Rechazada VARCHAR(1000),
    Cantidad_Proceso VARCHAR(1000),
    FOREIGN KEY (Id_ProductoAU) REFERENCES Productos(Id_ProductoAU)
);

-- TABLA CALIDADES
CREATE TABLE Calidades (
    SAG_CodigoAU INT PRIMARY KEY IDENTITY(1,1),
    SAG_Codigo VARCHAR(50),
    SAG_Codigo_Packing VARCHAR(50),
    Bins_Recepcionados VARCHAR(50),
    Bins_Vaciados VARCHAR(50),
    Cajas_Calidad VARCHAR(50)
);

-- TABLA CAMARAS
CREATE TABLE Camaras (
    Id_CamaraAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Camara VARCHAR(50),
    Nombre_Camara VARCHAR(100),
    Capacidad VARCHAR(1000)
);

-- TABLA FRIGORIFICOS
CREATE TABLE Frigorificos (
    Id_FrigorificoAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Frigorifico VARCHAR(50),
    Id_CamaraAU INT,
	Id_Camara VARCHAR(1000),
    Fecha_Entrada VARCHAR(1000), 
    Fecha_Salida VARCHAR(1000),
    Hora_Entrada VARCHAR(1000),
    Hora_Salida VARCHAR(1000),
    FOREIGN KEY (Id_CamaraAU) REFERENCES Camaras(Id_CamaraAU)
);

-- TABLA CAMBIOS
CREATE TABLE Cambios (
    Id_CambioAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Cambio VARCHAR(1000),
    Id_CategoriaAU INT,
    Nombre_Error VARCHAR(100),
    Especie_Error VARCHAR(100),
	Id_Categoria VARCHAR(1000),
    FOREIGN KEY (Id_CategoriaAU) REFERENCES Categorias(Id_CategoriaAU)
);

-- TABLA FECHAS
CREATE TABLE Fechas (
    Id_FechaAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Fecha VARCHAR(50),
    Dia VARCHAR(1000),
    Hora VARCHAR(100),
    Mes VARCHAR(20),
    Semestre VARCHAR(50),
    Año VARCHAR(1000),
    Temporada VARCHAR(50),
    Trimestre VARCHAR(50)
);

-- TABLA LOTES
CREATE TABLE Lotes (
    Id_LoteAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Lote VARCHAR(50),
    N_Cuartel VARCHAR(50),
    N_Lote VARCHAR(50),
    Transporte VARCHAR(100),
    Origen VARCHAR(100),
    Fecha_cosecha VARCHAR(1000)
);

-- TABLA SUCURSALES
CREATE TABLE Sucursales (
    Id_SucursalAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Sucursal VARCHAR(50),
    Nombre_Cliente VARCHAR(100),
	Region VARCHAR(200),
	Pais VARCHAR(500),
	Continente VARCHAR(500)
);

-- TABLA PACKINGS
CREATE TABLE Packings (
    Id_PackingAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Packing VARCHAR(50),
    Linea_Proceso VARCHAR(100),
    Tipo_Salida VARCHAR(50),
    Fecha_Salida VARCHAR(1000),
	Hora_Despacho VARCHAR(1000),
    Id_SucursalAU INT,
	Id_SucursaL VARCHAR(100),
    FOREIGN KEY (Id_SucursalAU) REFERENCES Sucursales(Id_SucursalAU)
);

-- TABLA DE HECHOS
CREATE TABLE Hechos (
    Id_HechosAU INT PRIMARY KEY IDENTITY(1,1),
    Id_Hecho VARCHAR(50),
    Id_FechaAU INT,
    SAG_CodigoAU INT,
    Id_CambioAU INT,
    Id_ProcesoAU INT,
    Id_LoteAU INT,
    Id_FrigorificoAU INT,
    Id_PackingAU INT,

	Id_Fecha VARCHAR(50),
	SAG_Codigo VARCHAR(50),
	Id_Cambio VARCHAR(50),
	Id_Proceso VARCHAR(50),
	Id_Lote VARCHAR(50),
	Id_Frigorifico VARCHAR(50),
	Id_Packing VARCHAR(50),

    Tratamiento VARCHAR(1000),
    Productividad VARCHAR(1000),
    Rechazo VARCHAR(1000),
    Sanitario VARCHAR(1000),
    Indicador_Bins VARCHAR(1000),
    Ciclo VARCHAR(1000),
    Capacidad VARCHAR(100),
	Enfriamiento VARCHAR(100),
	Exportar VARCHAR(100),
	Error VARCHAR(100),

    FOREIGN KEY (Id_FechaAU) REFERENCES Fechas(Id_FechaAU),
    FOREIGN KEY (SAG_CodigoAU) REFERENCES Calidades(SAG_CodigoAU),
    FOREIGN KEY (Id_CambioAU) REFERENCES Cambios(Id_CambioAU),
    FOREIGN KEY (Id_ProcesoAU) REFERENCES Procesos(Id_ProcesoAU),
    FOREIGN KEY (Id_LoteAU) REFERENCES Lotes(Id_LoteAU),
    FOREIGN KEY (Id_FrigorificoAU) REFERENCES Frigorificos(Id_FrigorificoAU),
    FOREIGN KEY (Id_PackingAU) REFERENCES Packings(Id_PackingAU)
	);
