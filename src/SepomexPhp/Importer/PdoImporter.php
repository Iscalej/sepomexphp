<?php

declare(strict_types=1);

namespace SepomexPhp\Importer;

use PDO;
use PDOStatement;
use SplFileObject;

/**
 * Import the sepomex raw file (iso-8859-1 encoded)
 * @package SepomexPhp\Importer
 */
class PdoImporter
{
    /** @var PDO */
    private $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Retrieve a list of common states renames, like 'Veracruz de Ignacio de la Llave' to 'Veracruz'
     *
     * @return array
     */
    public static function commonSatesRename(): array
    {
        return [
            'Coahuila de Zaragoza' => 'Coahuila',
            'Michoacán de Ocampo' => 'Michoacán',
            'Veracruz de Ignacio de la Llave' => 'Veracruz',
            'México' => 'Estado de México',
            'Ciudad de México'  => 'CDMX',
        ];
    }

    /**
     * Do the importation process from a raw file.
     * It is expected that the data structure is already created.
     *
     * @param string $rawfile
     * @param array|null $statesRename if null will use the common set of renames
     * @see commonSatesRename
     */
    public function import(string $rawfile, array $statesRename = null)
    {
        if (null === $statesRename) {
            $statesRename = $this->commonSatesRename();
        }
        $this->importRawTxt($rawfile);
        $this->populateStates();
        $this->renameStates($statesRename);
        $this->populateDistricts();
        $this->populateCities();
        $this->populateZipCodes();
        $this->populateLocationTypes();
        $this->populateLocations();
        $this->populatedir_asentamientos_codes();
        $this->clearRawTable();
    }

    public function createStruct()
    {
        $commands = [
            // raw
            'DROP TABLE IF EXISTS raw;',
            'CREATE TABLE raw (d_codigo text, d_asenta text, d_tipo_asenta text, d_mnpio text, d_estado text,'
                . ' d_ciudad text, d_cp text, c_estado text, c_oficina text, c_cp text, c_tipo_asenta text,'
                . ' c_mnpio text, id_asenta_cpcons text, d_zona text, c_cve_ciudad text);',
            // dir_estados
            'DROP TABLE IF EXISTS dir_estados;',
            'CREATE TABLE dir_estados (estaid integer primary key not null, es_nombre text not null);',
            // dir_municipios (autonumeric)
            'DROP TABLE IF EXISTS dir_municipios;',
            'CREATE TABLE dir_municipios (munid integer primary key autoincrement not null, mu_estaid integer not null,'
            . ' mu_nombre text not null, idraw text);',
            // dir_ciudades (autonumeric)
            'DROP TABLE IF EXISTS dir_ciudades;',
            'CREATE TABLE dir_ciudades (ciuid integer primary key autoincrement not null, ci_estaid integer not null,'
            . ' ci_nombre text not null, idraw text);',
            // dir_asentamientos_tipos
            'DROP TABLE IF EXISTS dir_asentamientos_tipos;',
            'CREATE TABLE dir_asentamientos_tipos (tipid integer primary key not null, ti_nombre text not null);',
            // dir_asentamientos
            'DROP TABLE IF EXISTS dir_asentamientos;',
            'CREATE TABLE dir_asentamientos (aseid integer primary key autoincrement not null, as_tipid integer not null,'
            . ' as_munid integer not null, as_ciuid integer default null, as_nombre text not null);',
            // dir_codes
            'DROP TABLE IF EXISTS dir_codes;',
            'CREATE TABLE dir_codes (coid integer primary key not null, co_munid int not null);',
            // dir_asentamientos_codes
            'DROP TABLE IF EXISTS dir_asentamientos_codes;',
            'CREATE TABLE dir_asentamientos_codes (ac_aseid integer not null, ac_coid integer not null,'
            . ' primary key(ac_aseid, ac_coid));',
        ];
        $this->execute(...$commands);
    }

    public function importRawTxt(string $filename)
    {
        if (! file_exists($filename) || ! is_readable($filename)) {
            throw new \RuntimeException("File $filename not found or not readable");
        }
        $sqlInsert = 'INSERT INTO raw VALUES (' . trim(str_repeat('?,', 15), ',') . ');';
        $stmt = $this->pdo->prepare($sqlInsert);
        $this->pdo->beginTransaction();
        $this->pdo->exec('DELETE FROM raw');
        $source = new SplFileObject($filename, 'r');
        foreach ($source as $i => $line) {
            // discard first lines
            if ($i < 2 || is_array($line) || ! $line) {
                continue;
            }
            $values = explode('|', iconv('iso-8859-1', 'utf-8', $line));
            $stmt->execute($values);
        }
        $this->pdo->commit();
    }

    public function populateStates()
    {
        $commands = [
            'DELETE FROM dir_estados;',
            'INSERT INTO dir_estados SELECT DISTINCT CAST(c_estado AS INTEGER) as estaid, d_estado as es_nombre'
            . ' FROM raw ORDER BY c_estado;',
        ];
        $this->execute(...$commands);
    }

    public function renameStates(array $names)
    {
        if (0 === count($names)) {
            return;
        }
        $sql = 'UPDATE dir_estados SET es_nombre = :newname WHERE (es_nombre = :oldname);';
        $stmt = $this->pdo->prepare($sql);
        foreach ($names as $oldname => $newname) {
            $stmt->execute(['oldname' => $oldname, 'newname' => $newname]);
        }
    }

    public function populateDistricts()
    {
        $commands = [
            'DELETE FROM dir_municipios;',
            'INSERT INTO dir_municipios SELECT DISTINCT null as munid, CAST(c_estado AS INTEGER) as mu_estaid, d_mnpio as mu_nombre,'
            . ' CAST(c_mnpio AS INTEGER) as idraw FROM raw ORDER BY c_estado, c_mnpio;',
        ];
        $this->execute(...$commands);
    }

    public function populateCities()
    {
        $commands = [
            'DELETE FROM dir_ciudades;',
            'INSERT INTO dir_ciudades SELECT DISTINCT null as ciuid, CAST(c_estado AS INTEGER) as ci_estaid, d_ciudad as ci_nombre,'
            . ' CAST(c_cve_ciudad AS INTEGER) as idraw FROM raw WHERE (d_ciudad <> "")'
            . ' ORDER BY c_estado, c_cve_ciudad;',
        ];
        $this->execute(...$commands);
    }

    public function populateLocationTypes()
    {
        $commands = [
            'DELETE FROM dir_asentamientos_tipos;',
            'INSERT INTO dir_asentamientos_tipos SELECT DISTINCT CAST(c_tipo_asenta AS INTEGER) AS tipid, d_tipo_asenta AS ti_nombre'
            . ' FROM raw ORDER BY c_tipo_asenta;',
        ];
        $this->execute(...$commands);
    }

    public function populateLocations()
    {
        $commands = [
            'DELETE FROM dir_asentamientos;',
            'INSERT INTO dir_asentamientos '
            . ' SELECT DISTINCT NULL AS id, t.tipid as aseidtype, d.munid AS iddistrict,'
            . ' c.ciuid AS idcity, d_asenta AS name'
            . ' FROM raw AS r'
            . ' INNER JOIN dir_asentamientos_tipos as t ON (t.ti_nombre = r.d_tipo_asenta)'
            . ' INNER JOIN dir_municipios as d'
            . ' ON (d.idraw = CAST(c_mnpio AS INTEGER) AND d.mu_estaid = CAST(c_estado AS INTEGER))'
            . ' LEFT JOIN dir_ciudades as c'
            . ' ON (c.idraw = CAST(c_cve_ciudad AS INTEGER) AND c.ci_estaid = CAST(c_estado AS INTEGER))'
            . ';',
        ];
        $this->execute(...$commands);
    }

    public function populateZipCodes()
    {
        $commands = [
            'DELETE FROM dir_codes;',
            'INSERT INTO dir_codes'
            . ' SELECT DISTINCT CAST(d_codigo AS INTEGER) AS coid, d.munid AS co_munid'
            . ' FROM raw AS r'
            . ' INNER JOIN dir_municipios AS d'
            . ' ON (d.idraw = CAST(c_mnpio AS INTEGER) AND d.mu_estaid = CAST(c_estado AS INTEGER))'
            . ';',
        ];
        $this->execute(...$commands);
    }

    public function populatedir_asentamientos_codes()
    {
        $commands = [
            'DELETE FROM dir_asentamientos_codes;',
            'INSERT INTO dir_asentamientos_codes'
            . ' SELECT DISTINCT l.aseid AS aseid, CAST(d_codigo AS INTEGER) AS as_code'
            . ' FROM raw AS r'
            . ' INNER JOIN dir_asentamientos_tipos AS t ON (t.ti_nombre = r.d_tipo_asenta)'
            . ' INNER JOIN dir_municipios AS d'
            . ' ON (d.idraw = CAST(c_mnpio AS INTEGER) AND d.mu_estaid = CAST(c_estado AS INTEGER))'
            . ' INNER JOIN dir_asentamientos AS l ON (t.tipid = l.as_tipid AND d.munid = l.as_munid AND l.as_nombre = r.d_asenta)'
            . ';',
        ];
        $this->execute(...$commands);
    }

    public function clearRawTable()
    {
        $this->execute('DELETE FROM raw;');
    }

    protected function execute(...$commands)
    {
        foreach ($commands as $command) {
            if (is_string($command)) {
                $this->pdo->exec($command);
            } elseif ($command instanceof PDOStatement) {
                $command->execute();
            }
        }
    }
}
